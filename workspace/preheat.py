"""
Capacity-aware preheat orchestration for roadside boxes (STRICTLY READ-ONLY
over the live map service and the mirror repository).

A dispatcher picks an **immutable mirror manifest** and a batch of target
boxes, sets a deadline, a per-box link rate cap and a minimum online
guarantee; the scheduler then arranges transfers according to:

  * **site shared bandwidth** — boxes are grouped into sites; the sum of
    granted rates on one site never exceeds the site capacity;
  * **box free disk** — a transfer only starts when the box can hold the
    remaining bytes; the disk reservation and the transfer slot are always
    acquired and released **as a pair**;
  * **content reuse** — objects already on the box (current mirror image or
    previously completed preheats) are never retransferred; a fully reused
    task completes instantly;
  * **fairness** — eligible-but-unserved tasks accrue debt (欠账) every tick;
    the scheduler serves the largest debt first, so a continuously eligible
    job can never starve;
  * **pause/resume** — pausing releases the (disk, slot) pair; resuming only
    sends the remaining bytes (bytes_done is never rewound).

No ghost occupancy: occupancy is *derived* from task states inside a single
atomic commit, so space shortage, link degradation, box offline, cancel or a
service crash can never leak a reservation. On startup the engine rebuilds
the queue, the debts and the remaining times from the persisted store: any
task caught mid-transfer is returned to the queue and its (disk, slot) pair
is released.

Quota changes, cancel and resubmission are deterministic: resubmitting an
identical job replays the first job (unless it was canceled — then a fresh
job is created); cancel is verdict-once. The web console and preheat_cli.py
read the same store and the same pure scheduling functions, so queue order,
rate-limit reasons and ETAs are identical.

**Box-side acceptance, quarantine and repair.** After a transfer completes
the staged bytes are NOT usable yet: the box must accept the manifest object
by object — block length, chunk sha256, per-object hash and finally the
whole-package root hash — and every run appends an *immutable* acceptance
record carrying the manifest generation, the job id and the box-local
acceptance sequence; a fully valid run mints an immutable, hash-chained
**proof**. Only proven content enters the reuse inventory and the box's
switchable set (可切换集合); the scheduler page and CLI render the same four
states: 待验收 pending / 已证明 proven / 隔离 quarantined / 修复中 repairing.

Missing or corrupt blocks isolate ONLY the affected objects: their chunks
are dropped and the corresponding disk is released, then exactly one repair
is created over the remaining blocks — proven objects are never retransferred
(proven objects merely riding along inside a repaired shared chunk are not
re-proven). Concurrent submissions with the same repair key return the first
repair task; an old-generation repair can never overwrite a newer-generation
proof (its proof is minted non-switchable / superseded). Box offline, service
restart and repair cancellation all release the (disk, slot) pair and the
flow resumes or converges with no stranded capacity. Writing a proof never
touches the manifest, the live effective map or any trip snapshot. Batch
queries summarize pass rates, quarantined capacity and failure reasons by
site and by manifest, and every acceptance/repair verdict is traceable.

This engine writes exclusively to data/preheat.json. It never moves region
generations, never touches patches/snapshots, and never mutates manifests.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import threading
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from engine import DomainError, canonical, sha256, short, utcnow_iso
from mirror import root_hash_of, sha256_bytes

KB = 1000.0                      # 1 KB = 1000 bytes (rates are KB/s)
EPS = 1e-6
TASK_OPEN = ("queued", "transferring")
JOB_OPEN = ("active", "paused")

# box-side acceptance states (验收状态 — same strings in web + CLI)
ACCEPT_PENDING = "pending"            # 待验收：传输完成，等待盒端逐项验证
ACCEPT_PROVEN = "proven"              # 已证明：不可变证明有效，进入复用与可切换集合
ACCEPT_QUARANTINED = "quarantined"    # 隔离：发现缺块/损坏，受影响对象已隔离
ACCEPT_REPAIRING = "repairing"        # 修复中：一次修复正在补齐剩余块
ACCEPT_SUPERSEDED = "superseded"      # 旧代次修复完成，不得覆盖新代次证明
ACCEPT_CANCELED = "canceled"
ACCEPT_CLOSED = (ACCEPT_PROVEN, ACCEPT_SUPERSEDED, ACCEPT_CANCELED)

# object/chunk verification failure codes
F_MISSING = "BLOCK_MISSING"                 # 缺块
F_LENGTH = "BLOCK_LENGTH_MISMATCH"          # 块长度不符
F_CHUNK_HASH = "CHUNK_HASH_MISMATCH"        # 块散列不符
F_OBJECT_HASH = "OBJECT_HASH_MISMATCH"      # 对象散列不符
F_DECOMPRESS = "BLOCK_UNREADABLE"           # 块无法解压
F_ROOT = "ROOT_HASH_MISMATCH"               # 根散列不符
F_SUPERSEDED = "SUPERSEDED_GENERATION"      # 旧代次不得覆盖新代次证明

ACCEPT_REASONS = {
    F_MISSING: "验收缺块：受影响对象已隔离并释放空间",
    F_LENGTH: "块长度与清单声明不符：受影响对象已隔离",
    F_CHUNK_HASH: "块散列与清单声明不符：受影响对象已隔离",
    F_OBJECT_HASH: "对象内容散列不符：受影响对象已隔离",
    F_DECOMPRESS: "块内容无法读取：受影响对象已隔离",
    F_ROOT: "整包根散列复核不一致：本次证明无效",
    F_SUPERSEDED: "修复属于旧代次，不得覆盖盒上更新代次的证明",
}

ACCEPT_STATE_CN = {
    ACCEPT_PENDING: "待验收",
    ACCEPT_PROVEN: "已证明",
    ACCEPT_QUARANTINED: "隔离",
    ACCEPT_REPAIRING: "修复中",
    ACCEPT_SUPERSEDED: "旧代次不可切换",
    ACCEPT_CANCELED: "已取消",
}

# limit / wait reason codes (same strings in web + CLI: same store, same code)
REASONS = {
    "BOX_OFFLINE": "盒子离线，传输名额与磁盘预留不会分配",
    "BOX_DISK_FULL": "盒内可用空间不足，等待释放或扩容",
    "LINK_DOWN": "链路速率上限为 0（链路降级），暂停分配",
    "SITE_SATURATED": "站点共享带宽被更高优先级（欠账更多）的作业占满，欠账累计中",
    "SITE_SHARED": "受站点共享带宽限制",
    "BOX_LINK": "受盒链路速率上限限制（链路降级）",
    "JOB_CAP": "受作业速率上限限制",
    "PAUSED": "作业已暂停，名额与预留已成对释放",
    "READY": "已获得传输名额与磁盘预留",
}


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _round6(x: float) -> float:
    return round(x, 6)


def _b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


class PreheatEngine:
    def __init__(self, path: str, mirror=None):
        self.path = path
        self.mirror = mirror            # MirrorEngine, used READ-ONLY
        self.lock = threading.RLock()
        self.state: dict = {}
        self._load()

    # ========================================================= persistence ==
    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
            for k, v in self._blank().items():
                self.state.setdefault(k, v)
        else:
            self.state = self._blank()
        self._migrate_locked()
        changed = self._recover_locked()
        before = json.dumps(self.state, sort_keys=True, ensure_ascii=False)
        self._settle(self.state)  # rebuild the schedule after any restart
        if changed or not os.path.exists(self.path) or \
                json.dumps(self.state, sort_keys=True, ensure_ascii=False) != before:
            self._persist_locked()

    @staticmethod
    def _blank() -> dict:
        return {"clock": utcnow_iso(),   # simulated clock, advanced by tick()
                "sites": {},             # site_id -> shared-bandwidth site
                "boxes": {},             # box_id -> roadside box record
                "jobs": {},              # job_id -> preheat job
                "job_by_key": {},        # content key -> job_id (deterministic resubmit)
                "tasks": {},             # task_id -> transfer / repair task
                "acceptances": {},       # acceptance_id -> immutable acceptance record
                "repairs": {},           # repair_id -> repair (isolation → one repair)
                "repair_by_key": {},     # repair key -> repair_id (concurrent → first)
                "switchable": {},        # box_id -> [eligible proof summaries]
                "events": [],            # append-only orchestration journal
                "seq": {"job": 0, "task": 0, "event": 0,
                        "acceptance": 0, "repair": 0}}

    def _persist_locked(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def commit(self, mutate) -> Any:
        """Run mutate(copy), re-settle allocation, atomically persist.

        Every committed state satisfies: per-site granted rate ≤ capacity and
        per-box reserved disk ≤ free space — concurrency can never break the
        caps because allocation is recomputed inside the same atomic commit.
        """
        with self.lock:
            draft = copy.deepcopy(self.state)
            result = mutate(draft)
            self._settle(draft)
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(draft, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            self.state = draft
            return result

    def _migrate_locked(self):
        """Backfill box-side acceptance fields on stores written by older code."""
        for b in self.state["boxes"].values():
            b.setdefault("quarantine", {})
            b.setdefault("proven_bytes", 0.0)
            b.setdefault("acceptance_seq", 0)
        for t in self.state["tasks"].values():
            t.setdefault("kind", "transfer")
            t.setdefault("accept_state",
                         ACCEPT_PROVEN if t["status"] == "done" else ACCEPT_PENDING)
            t.setdefault("staged", None)
            t.setdefault("damage", None)
            t.setdefault("proof_id", None)
            t.setdefault("last_acceptance_id", None)
            t.setdefault("parent_task_id", None)
            t.setdefault("repair_id", None)
            t.setdefault("repair_result", None)
            t.setdefault("chunk_indices", [])
        self.state.setdefault("acceptances", {})
        self.state.setdefault("repairs", {})
        self.state.setdefault("repair_by_key", {})
        self.state.setdefault("switchable", {})
        self.state["seq"].setdefault("acceptance", 0)
        self.state["seq"].setdefault("repair", 0)

    def _recover_locked(self) -> bool:
        """Rebuild queue/debts/remaining after a (crash) restart.

        Any task caught mid-transfer held a (disk reservation, transfer slot)
        pair; the service is gone, so the pair is released and the task is
        re-queued — resume only sends the remaining bytes. Debts persist.
        The caller immediately re-settles, so still-eligible tasks re-acquire
        their pairs deterministically; nothing leaks either way.
        """
        crashed = [t for t in self.state["tasks"].values()
                   if t["status"] == "transferring"]
        if not crashed:
            return False
        for t in crashed:
            t["status"] = "queued"
            t["reserved_bytes"] = 0.0
            self._journal(self.state, t, "RECOVERED",
                          "服务重启：传输名额与磁盘预留已成对释放（无幽灵占用），"
                          f"续传只补剩余 {self._remaining(t):.0f} 字节；欠账保留")
            t["updated_at"] = self.state["clock"]
        self._event_locked(self.state, "SERVICE_RECOVERED",
                           f"服务重启恢复：{len(crashed)} 个传输中任务重新排队，"
                           f"磁盘预留与传输名额成对释放，队列/欠账/剩余时长已重建",
                           crashed[0]["job_id"])
        return True

    # ============================================================= helpers ==
    def _next(self, s: dict, key: str, prefix: str) -> str:
        s["seq"][key] += 1
        return f"{prefix}-{s['seq'][key]:04d}"

    def _clock(self, s: dict) -> datetime:
        return _parse(s["clock"])

    def _event_locked(self, s: dict, etype: str, summary: str, ref: str) -> dict:
        s["seq"]["event"] += 1
        ev = {"event_id": s["seq"]["event"], "ts": s["clock"],
              "type": etype, "summary": summary, "ref": ref}
        s["events"].append(ev)
        return ev

    def _journal(self, s: dict, task: dict, event: str, detail: str):
        task["journal"].append({"ts": s["clock"], "event": event, "detail": detail})

    @staticmethod
    def _remaining(t: dict) -> float:
        return max(0.0, t["bytes_total"] - t["bytes_done"])

    def list_events(self, limit: int = 80) -> list:
        with self.lock:
            return copy.deepcopy(self.state["events"][-limit:][::-1])

    # ==================================================== mirror (read-only)
    def _manifest(self, manifest_id: str) -> dict:
        """Read-only lookup of an immutable manifest in the mirror store."""
        if self.mirror is None:
            raise DomainError("PREHEAT_MIRROR_UNAVAILABLE",
                              "镜像仓库不可用，无法读取内容清单", 409)
        with self.mirror.lock:
            man = self.mirror.state.get("manifests", {}).get(manifest_id)
            if not man:
                raise DomainError("PREHEAT_MANIFEST_UNKNOWN",
                                  f"清单 {manifest_id} 不在镜像仓库中；请先在"
                                  f"镜像分发页生成不可变清单", 404)
            return copy.deepcopy(man)

    # ====================================================== sites & boxes ==
    def upsert_site(self, site_id: str, capacity_kbps: float) -> dict:
        site_id = (site_id or "").strip()
        if not site_id:
            raise DomainError("BAD_REQUEST", "站点编号不能为空", 400)
        try:
            cap = float(capacity_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "站点带宽必须是数字（KB/s）", 400)
        if cap <= 0:
            raise DomainError("BAD_REQUEST", "站点共享带宽必须 > 0", 400)

        def m(s):
            site = s["sites"].get(site_id)
            if site is None:
                site = {"site_id": site_id, "capacity_kbps": cap,
                        "created_at": s["clock"], "updated_at": s["clock"]}
                s["sites"][site_id] = site
                self._event_locked(s, "SITE_REGISTERED",
                                   f"注册站点 {site_id}（共享带宽 {cap:g} KB/s）", site_id)
            elif abs(site["capacity_kbps"] - cap) > EPS:
                old = site["capacity_kbps"]
                site["capacity_kbps"] = cap
                site["updated_at"] = s["clock"]
                self._event_locked(
                    s, "QUOTA_CHANGED",
                    f"站点 {site_id} 共享带宽 {old:g} → {cap:g} KB/s；"
                    f"超出部分将按优先级确定性抢占", site_id)
            return site_id

        self.commit(m)
        return self.get_site(site_id)

    def set_site_capacity(self, site_id: str, capacity_kbps: float) -> dict:
        with self.lock:
            if site_id not in self.state["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册", 404)
        return self.upsert_site(site_id, capacity_kbps)

    def upsert_box(self, box_id: str, site_id: str, disk_total_mb: float,
                   link_cap_kbps: float, disk_used_mb: float = 0.0) -> dict:
        box_id = (box_id or "").strip()
        site_id = (site_id or "").strip()
        if not box_id or not site_id:
            raise DomainError("BAD_REQUEST", "盒子与站点编号不能为空", 400)
        try:
            disk_total = float(disk_total_mb) * 1e6
            used = float(disk_used_mb) * 1e6
            cap = float(link_cap_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "磁盘/链路参数必须是数字", 400)
        if disk_total <= 0 or used < 0 or used > disk_total or cap < 0:
            raise DomainError("BAD_REQUEST",
                              "磁盘总量必须 > 0、0 ≤ 已用 ≤ 总量、链路速率 ≥ 0", 400)
        with self.lock:
            if site_id not in self.state["sites"]:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册，请先注册站点", 404)

        def m(s):
            box = s["boxes"].get(box_id)
            if box is None:
                inventory = self._seed_inventory(box_id)
                box = {"box_id": box_id, "site_id": site_id,
                       "disk_total_bytes": disk_total, "disk_used_bytes": used,
                       "link_cap_kbps": cap, "online": True,
                       "inventory": inventory,
                       "proven_bytes": 0.0,   # bytes covered by valid proofs
                       "quarantine": {},       # object_id -> {hash, size, since, reason}
                       "acceptance_seq": 0,    # box-local monotonic acceptance number
                       "created_at": s["clock"], "updated_at": s["clock"]}
                s["boxes"][box_id] = box
                self._event_locked(
                    s, "BOX_REGISTERED",
                    f"注册盒子 {box_id} @ {site_id}（磁盘 {disk_total / 1e6:g} MB，"
                    f"链路 {cap:g} KB/s，库存对象 {len(inventory)} 个）", box_id)
            else:
                changes = []
                if box["site_id"] != site_id:
                    changes.append(f"站点 {box['site_id']} → {site_id}")
                    box["site_id"] = site_id
                if abs(box["disk_total_bytes"] - disk_total) > EPS:
                    changes.append(
                        f"磁盘 {box['disk_total_bytes'] / 1e6:g} → {disk_total / 1e6:g} MB")
                    box["disk_total_bytes"] = disk_total
                if abs(box["disk_used_bytes"] - used) > EPS:
                    changes.append(
                        f"已用 {box['disk_used_bytes'] / 1e6:g} → {used / 1e6:g} MB")
                    box["disk_used_bytes"] = used
                if abs(box["link_cap_kbps"] - cap) > EPS:
                    changes.append(
                        f"链路 {box['link_cap_kbps']:g} → {cap:g} KB/s")
                    box["link_cap_kbps"] = cap
                if changes:
                    box["updated_at"] = s["clock"]
                    self._event_locked(s, "BOX_UPDATED",
                                       f"盒子 {box_id} 配置更新：" + "，".join(changes),
                                       box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    def _seed_inventory(self, box_id: str) -> list:
        """Content reuse seed: objects of the box's current mirror image."""
        if self.mirror is None:
            return []
        with self.mirror.lock:
            dev = self.mirror.state.get("devices", {}).get(box_id)
            img = (dev or {}).get("current_image")
            if not img:
                return []
            man = self.mirror.state.get("manifests", {}).get(img.get("manifest_id"))
            if not man:
                return []
            return sorted(o["hash"] for o in man.get("objects", []))

    def set_box_link(self, box_id: str, link_cap_kbps: float) -> dict:
        try:
            cap = float(link_cap_kbps)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "链路速率必须是数字（KB/s）", 400)
        if cap < 0:
            raise DomainError("BAD_REQUEST", "链路速率上限不能为负", 400)

        def m(s):
            box = s["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN", f"盒子 {box_id} 未注册", 404)
            old = box["link_cap_kbps"]
            box["link_cap_kbps"] = cap
            box["updated_at"] = s["clock"]
            kind = "LINK_DEGRADED" if cap < old else "LINK_RESTORED"
            self._event_locked(
                s, kind,
                f"盒子 {box_id} 链路速率上限 {old:g} → {cap:g} KB/s"
                + ("（链路降级，超出部分确定性让路）" if cap < old else "（链路恢复）"),
                box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    def set_box_online(self, box_id: str, online: bool) -> dict:
        def m(s):
            box = s["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN", f"盒子 {box_id} 未注册", 404)
            if box["online"] == bool(online):
                return box_id
            box["online"] = bool(online)
            box["updated_at"] = s["clock"]
            self._event_locked(
                s, "BOX_OFFLINE" if not online else "BOX_ONLINE",
                f"盒子 {box_id} {'离线：传输名额与磁盘预留成对释放，不留幽灵占用' if not online else '恢复在线：重新排队' }",
                box_id)
            return box_id

        self.commit(m)
        return self.get_box(box_id)

    # ============================================================ content ==
    def _needed(self, s: dict, manifest: dict, box: dict):
        """Chunks/bytes this box still needs: chunks holding ≥1 object not in
        the *proven* inventory. Returns (needed_bytes, total_bytes, indices)."""
        inv = set(box["inventory"])
        total = sum(c["size"] for c in manifest["chunks"])
        needed, indices = 0.0, []
        for c in manifest["chunks"]:
            objs = [o for o in manifest["objects"] if o["object_id"] in c["objects"]]
            if any(o["hash"] not in inv for o in objs):
                needed += c["size"]
                indices.append(c["index"])
        return needed, total, indices

    def _needed_bytes(self, s: dict, manifest: dict, box: dict):
        needed, total, _ = self._needed(s, manifest, box)
        return needed, total

    def _staged_bytes(self, s: dict, t: dict) -> float:
        return sum(len(_b64d(c["data_b64"]))
                   for c in (t.get("staged") or {}).values())

    # ---------------------------------------------------------------- mirror
    def _chunk_blob(self, manifest_hash: str, index: int) -> Optional[bytes]:
        """Read-only fetch of a compressed chunk blob from the mirror store."""
        if self.mirror is None:
            return None
        with self.mirror.lock:
            rec = self.mirror.state.get("chunk_store", {}).get(
                f"{manifest_hash}:{index}")
            return _b64d(rec["data_b64"]) if rec else None

    # ============================================================== submit ==
    def create_job(self, body: dict) -> dict:
        manifest_id = (body.get("manifest_id") or "").strip()
        man = self._manifest(manifest_id)  # read-only; 404 if unknown
        targets = body.get("targets")
        if not isinstance(targets, list) or not targets or \
                not all(isinstance(t, str) and t.strip() for t in targets):
            raise DomainError("PREHEAT_NO_TARGETS",
                              "targets 必须是非空盒子编号数组", 400)
        targets = list(dict.fromkeys(t.strip() for t in targets))
        try:
            link_cap = float(body.get("link_cap_kbps", 1024))
            min_online = int(body.get("min_online", len(targets)))
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "速率/最小在线参数非法", 400)
        if link_cap <= 0:
            raise DomainError("BAD_REQUEST", "作业链路速率上限必须 > 0", 400)
        min_online = max(1, min(len(targets), min_online))
        note = body.get("note", "")
        created_by = body.get("created_by", "dispatcher")

        with self.lock:
            missing = [t for t in targets if t not in self.state["boxes"]]
            if missing:
                raise DomainError(
                    "PREHEAT_BOX_UNKNOWN",
                    f"目标盒子未注册：{', '.join(missing)}；请先注册盒子", 404)
            clock = self._clock(self.state)
            if body.get("deadline"):
                try:
                    deadline = _parse(body["deadline"])
                except ValueError:
                    raise DomainError("BAD_REQUEST", "deadline 必须是 ISO 时间", 400)
            else:
                secs = float(body.get("deadline_in_sec", 3600))
                if secs <= 0:
                    raise DomainError("BAD_REQUEST", "截止时间必须在未来", 400)
                deadline = clock + timedelta(seconds=secs)
            key = sha256({"manifest_hash": man["manifest_hash"],
                          "targets": sorted(targets),
                          "deadline": _iso(deadline),
                          "link_cap_kbps": link_cap,
                          "min_online": min_online, "note": note})
            existing = self.state["job_by_key"].get(key)
            if existing and self.state["jobs"][existing]["status"] != "canceled":
                # deterministic resubmit: identical request replays the first job
                return {"job": self._job_view(self.state, self.state["jobs"][existing],
                                              idempotent=True),
                        "idempotent": True}

        def m(s):
            jid = self._next(s, "job", "PJ")
            job = {"job_id": jid, "seq": s["seq"]["job"], "job_key": key,
                   "manifest_id": man["manifest_id"],
                   "manifest_hash": man["manifest_hash"],
                   "version": man["source"]["version"],
                   "targets": list(targets), "deadline": _iso(deadline),
                   "link_cap_kbps": link_cap, "min_online": min_online,
                   "note": note, "status": "active", "cancel_result": None,
                   "created_by": created_by,
                   "created_at": s["clock"], "updated_at": s["clock"]}
            s["jobs"][jid] = job
            s["job_by_key"][key] = jid
            tasks = []
            for bid in targets:
                tid = self._next(s, "task", "PT")
                box = s["boxes"][bid]
                needed, total, indices = self._needed(s, man, box)
                task = {"task_id": tid, "seq": s["seq"]["task"], "job_id": jid,
                        "kind": "transfer", "box_id": bid, "site_id": box["site_id"],
                        "status": "queued",
                        "manifest_id": man["manifest_id"],
                        "manifest_hash": man["manifest_hash"],
                        "accept_state": ACCEPT_PENDING,
                        "staged": None,           # str(index) -> {size, hash, data_b64}
                        "damage": None,           # fault injection for the next accept
                        "proof_id": None,
                        "last_acceptance_id": None,
                        "parent_task_id": None,
                        "repair_id": None,
                        "repair_result": None,
                        "chunk_indices": indices,
                        "bytes_total": needed, "bytes_done": 0.0,
                        "reserved_bytes": 0.0, "debt_kb": 0.0,
                        "reuse_ratio": (1.0 - needed / total) if total > EPS else 1.0,
                        "journal": [], "created_at": s["clock"],
                        "updated_at": s["clock"], "done_at": None}
                self._journal(s, task, "TASK_QUEUED",
                              f"作业 {jid} 下发：需传输 {needed:.0f}/{total:.0f} 字节"
                              f"（内容复用率 {(task['reuse_ratio'] * 100):.0f}%）")
                s["tasks"][tid] = task
                tasks.append(tid)
            job["tasks"] = tasks
            self._event_locked(
                s, "JOB_SUBMITTED",
                f"预热作业 {jid}：清单 {man['manifest_id']}（{man['source']['version']}）"
                f"→ {len(targets)} 台盒子，截止 {job['deadline']}，"
                f"链路 ≤{link_cap:g} KB/s，最小在线保障 {min_online}/{len(targets)}",
                jid)
            return jid

        jid = self.commit(m)
        with self.lock:
            return {"job": self._job_view(self.state, self.state["jobs"][jid],
                                          idempotent=False),
                    "idempotent": False}

    # ====================================================== job lifecycle ==
    def _get_job(self, s: dict, job_id: str) -> dict:
        job = s["jobs"].get(job_id)
        if not job:
            raise DomainError("PREHEAT_JOB_UNKNOWN", f"作业 {job_id} 不存在", 404)
        return job

    def cancel_job(self, job_id: str, by: str = "dispatcher") -> dict:
        """Verdict-once: re-canceling replays the first cancel result."""
        with self.lock:
            job0 = self._get_job(self.state, job_id)
            if job0["status"] == "canceled":
                return {"job_id": job_id, "status": "canceled",
                        "result": copy.deepcopy(job0["cancel_result"]),
                        "idempotent": True}
            if job0["status"] == "completed":
                raise DomainError("PREHEAT_JOB_CLOSED",
                                  f"作业 {job_id} 已完成，不能取消", 409)

        def m(s):
            job = s["jobs"][job_id]
            released = 0
            tids = list(job["tasks"]) + self._repair_tids(s, job_id)
            for tid in tids:
                t = s["tasks"][tid]
                open_pair = t["status"] in TASK_OPEN + ("paused",)
                unclosed_done = t["status"] == "done" and \
                    t["accept_state"] not in ACCEPT_CLOSED
                if not (open_pair or unclosed_done):
                    continue
                if t["reserved_bytes"] > EPS:
                    released += 1
                if open_pair:
                    t["status"] = "canceled"
                t["reserved_bytes"] = 0.0
                t["updated_at"] = s["clock"]
                if t["kind"] == "repair":
                    self._release_staging(s, t, "作业取消")
                    if t["status"] != "canceled":
                        t["status"] = "canceled"
                    self._close_repair_locked(
                        s, t, {"outcome": "canceled", "at": s["clock"],
                               "by": by}, journal="REPAIR_CANCELED")
                    parent = s["tasks"].get(t["parent_task_id"])
                    if parent is not None:
                        parent["repair_id"] = None
                        parent["accept_state"] = ACCEPT_CANCELED
                        parent["status"] = "canceled"
                        self._release_staging(
                            s, parent, "作业取消：父任务暂存随修复成对清除")
                else:
                    t["accept_state"] = ACCEPT_CANCELED
                    t["status"] = "canceled"
                    self._release_staging(s, t, "作业取消：暂存内容随预留成对清除")
                    self._journal(s, t, "TASK_CANCELED",
                                  "作业取消：磁盘预留与传输名额成对释放，"
                                  "暂存/隔离修复数据已清除，不留幽灵占用")
            job["status"] = "canceled"
            job["updated_at"] = s["clock"]
            result = {"outcome": "canceled", "by": by, "at": s["clock"],
                      "released_pairs": released}
            job["cancel_result"] = result
            self._event_locked(
                s, "JOB_CANCELED",
                f"作业 {job_id} 已取消：{released} 对磁盘预留/传输名额成对释放", job_id)
            return copy.deepcopy(result)

        result = self.commit(m)
        return {"job_id": job_id, "status": "canceled",
                "result": result, "idempotent": False}

    def pause_job(self, job_id: str) -> dict:
        return self._pause_resume(job_id, pause=True)

    def resume_job(self, job_id: str) -> dict:
        return self._pause_resume(job_id, pause=False)

    def _pause_resume(self, job_id: str, pause: bool) -> dict:
        with self.lock:
            job0 = self._get_job(self.state, job_id)
            want = "paused" if pause else "active"
            if job0["status"] == want:
                return {"job_id": job_id, "status": want, "idempotent": True}
            if job0["status"] in ("canceled", "completed"):
                raise DomainError(
                    "PREHEAT_JOB_CLOSED",
                    f"作业 {job_id} 已终结（{job0['status']}），"
                    f"{'暂停' if pause else '恢复'}被拒绝", 409)

        def m(s):
            job = s["jobs"][job_id]
            job["status"] = "paused" if pause else "active"
            job["updated_at"] = s["clock"]
            tids = list(job["tasks"]) + self._repair_tids(s, job_id)
            for tid in tids:
                t = s["tasks"][tid]
                if pause and t["status"] in TASK_OPEN:
                    t["status"] = "paused"
                    t["reserved_bytes"] = 0.0
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_PAUSED",
                                  "作业暂停：磁盘预留与传输名额成对释放；"
                                  "恢复时只补剩余数据")
                elif not pause and t["status"] == "paused":
                    t["status"] = "queued"
                    t["updated_at"] = s["clock"]
                    self._journal(s, t, "TASK_RESUMED",
                                  f"作业恢复：重新排队，只补剩余 "
                                  f"{self._remaining(t):.0f} 字节")
            self._event_locked(
                s, "JOB_PAUSED" if pause else "JOB_RESUMED",
                f"作业 {job_id} {'已暂停（名额/预留成对释放）' if pause else '已恢复（只补剩余数据）'}",
                job_id)
            return job["status"]

        status = self.commit(m)
        return {"job_id": job_id, "status": status, "idempotent": False}

    # ============================================================ the plan ==
    def _open_tasks(self, s: dict) -> list:
        return [t for t in s["tasks"].values()
                if t["status"] in TASK_OPEN
                and s["jobs"][t["job_id"]]["status"] == "active"]

    def _want(self, s: dict, t: dict) -> float:
        job = s["jobs"][t["job_id"]]
        box = s["boxes"][t["box_id"]]
        return min(job["link_cap_kbps"], box["link_cap_kbps"])

    @staticmethod
    def _priority(s: dict):
        def key(t):
            job = s["jobs"][t["job_id"]]
            return (-t["debt_kb"], job["deadline"], job["seq"], t["seq"])
        return key

    def _plan(self, s: dict, remaining: Optional[dict] = None) -> dict:
        """Pure scheduling function: {task_id: {grant_kbps, code, message}}.

        Greedy by (debt desc, deadline, seq) within each site; the disk
        reservation and the transfer slot are granted together, never apart.
        """
        plan: dict[str, dict] = {}
        by_site: dict[str, list] = {}
        for t in self._open_tasks(s):
            by_site.setdefault(t["site_id"], []).append(t)
        for site_id, tasks in by_site.items():
            site = s["sites"].get(site_id)
            if site is None:
                continue
            budget = site["capacity_kbps"]
            reserved: dict[str, float] = {}
            for t in sorted(tasks, key=self._priority(s)):
                tid = t["task_id"]
                box = s["boxes"][t["box_id"]]
                rem = (remaining.get(tid, 0.0) if remaining is not None
                       else self._remaining(t))
                if rem <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "READY",
                                 "message": REASONS["READY"]}
                    continue
                if not box["online"]:
                    plan[tid] = {"grant_kbps": 0.0, "code": "BOX_OFFLINE",
                                 "message": REASONS["BOX_OFFLINE"]}
                    continue
                free = (box["disk_total_bytes"] - box["disk_used_bytes"]
                        - reserved.get(t["box_id"], 0.0))
                if rem > free + EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "BOX_DISK_FULL",
                                 "message": REASONS["BOX_DISK_FULL"]}
                    continue
                want = self._want(s, t)
                if want <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "LINK_DOWN",
                                 "message": REASONS["LINK_DOWN"]}
                    continue
                grant = min(want, budget)
                if grant <= EPS:
                    plan[tid] = {"grant_kbps": 0.0, "code": "SITE_SATURATED",
                                 "message": REASONS["SITE_SATURATED"]}
                    continue
                budget -= grant
                reserved[t["box_id"]] = reserved.get(t["box_id"], 0.0) + rem
                if grant < want - EPS:
                    code = "SITE_SHARED"
                elif box["link_cap_kbps"] < s["jobs"][t["job_id"]]["link_cap_kbps"] - EPS:
                    code = "BOX_LINK"
                else:
                    code = "JOB_CAP"
                plan[tid] = {"grant_kbps": _round6(grant), "code": code,
                             "message": REASONS[code]}
        return plan

    def _settle(self, s: dict):
        """Apply the plan to the state: acquire/release (disk, slot) pairs."""
        # content reuse: unstarted TRANSFER tasks shrink as the box's proven
        # inventory grows; a fully reused transfer completes instantly without
        # any transfer. Repair tasks never shrink: their block set is fixed at
        # creation (一次修复按剩余块创建，不能重传已证明对象).
        changed = True
        while changed:
            changed = False
            for t in sorted(self._open_tasks(s), key=lambda x: x["seq"]):
                if t["kind"] != "transfer" or t["bytes_done"] > EPS \
                        or t["status"] != "queued":
                    continue
                man = self._manifest_ref(s, t)
                if man is None:
                    continue
                box = s["boxes"][t["box_id"]]
                needed, total, indices = self._needed(s, man, box)
                if abs(needed - t["bytes_total"]) > EPS:
                    t["bytes_total"] = needed
                    t["chunk_indices"] = indices
                    t["reuse_ratio"] = (1.0 - needed / total) if total > EPS else 1.0
                    t["updated_at"] = s["clock"]
                    changed = True
                if needed <= EPS:
                    self._complete_task(s, t, reused=True)
                    changed = True
        plan = self._plan(s)
        for t in self._open_tasks(s):
            p = plan.get(t["task_id"])
            granted = p is not None and p["grant_kbps"] > EPS
            if granted and t["status"] != "transferring":
                t["status"] = "transferring"
                t["reserved_bytes"] = self._remaining(t)
                t["updated_at"] = s["clock"]
                self._journal(s, t, "PAIR_ACQUIRED",
                              f"成对取得磁盘预留 {self._remaining(t):.0f} 字节 + "
                              f"传输名额（{p['grant_kbps']:g} KB/s，{p['message']}）")
            elif granted:
                t["reserved_bytes"] = self._remaining(t)
            elif not granted and t["status"] == "transferring":
                t["status"] = "queued"
                t["reserved_bytes"] = 0.0
                t["updated_at"] = s["clock"]
                self._journal(s, t, "PAIR_RELEASED",
                              f"成对释放磁盘预留与传输名额（{p['message'] if p else '计划外'}）；"
                              f"已传 {t['bytes_done']:.0f} 字节保留，续传只补剩余")
        # job completion — gated on box-side acceptance: only proven /
        # superseded / canceled primary tasks close the job. A box sitting in
        # quarantine or repair keeps the job open.
        for job in s["jobs"].values():
            if job["status"] in JOB_OPEN and job.get("tasks") and all(
                    self._primary_closed(s, s["tasks"][tid])
                    for tid in job["tasks"]):
                job["status"] = "completed"
                job["updated_at"] = s["clock"]
                proven = sum(1 for tid in job["tasks"]
                             if s["tasks"][tid]["accept_state"] in
                             (ACCEPT_PROVEN, ACCEPT_SUPERSEDED))
                self._event_locked(
                    s, "JOB_COMPLETED",
                    f"作业 {job['job_id']} 收口：{proven}/{len(job['tasks'])} 台盒子"
                    f"已通过盒端验收出证（其余取消），仅已证明内容计入复用",
                    job["job_id"])

    @staticmethod
    def _primary_closed(s: dict, t: dict) -> bool:
        if t["status"] == "canceled":
            return True
        return t["status"] == "done" and t["accept_state"] in ACCEPT_CLOSED

    @staticmethod
    def _repair_tids(s: dict, job_id: str) -> list:
        return [tid for tid, t in s["tasks"].items()
                if t.get("kind") == "repair" and t["job_id"] == job_id]

    def _manifest_ref(self, s: dict, t: dict) -> Optional[dict]:
        """Manifest for a task, read-only from the mirror store."""
        if self.mirror is None:
            return None
        with self.mirror.lock:
            return self.mirror.state.get("manifests", {}).get(
                t.get("manifest_id") or t["manifest_hash"])

    def _complete_task(self, s: dict, t: dict, reused: bool = False):
        """Transfer (or repair) bytes finished.

        Transferred bytes are staged on the box and consume real disk; the
        pair (reservation, slot) is released and the task waits for box-side
        acceptance (待验收). A 100%-reuse transfer has no staged bytes: the
        objects are already proven content, so the acceptance is auto-minted
        straight away (already proven — never retransferred, re-proven only as
        a manifest-level proof).
        """
        box = s["boxes"][t["box_id"]]
        man = self._manifest_ref(s, t)
        t["status"] = "done"
        t["bytes_done"] = t["bytes_total"]
        t["reserved_bytes"] = 0.0
        t["done_at"] = s["clock"]
        t["updated_at"] = s["clock"]
        if reused:
            pass  # fully reused transfer: no bytes were moved
        else:
            staged = {}
            indices = list(t.get("chunk_indices") or [])
            for idx in indices:
                blob = self._chunk_blob(t["manifest_hash"], idx)
                if blob is None:
                    continue
                staged[str(idx)] = {
                    "size": len(blob), "hash": sha256_bytes(blob),
                    "data_b64": base64.b64encode(blob).decode("ascii")}
            t["staged"] = staged or None
            armed_missing = {int(x) for x in
                             ((t.get("damage") or {}).get("missing") or [])}
            self._apply_damage_locked(s, t, t.get("damage"))
            # missing blocks were never retained on the box: they consume no
            # disk and will be isolated/repaired at acceptance
            box["disk_used_bytes"] += self._staged_bytes(s, t)
            box["updated_at"] = s["clock"]
            t["accept_state"] = ACCEPT_PENDING
            self._journal(
                s, t, "TRANSFER_STAGED",
                f"传输完成：{self._staged_bytes(s, t):.0f} 字节暂存盒端等待验收"
                + (f"（模拟缺块 {sorted(armed_missing)}，对应空间未占用）"
                   if armed_missing else "")
                + "，磁盘预留与传输名额成对释放；验收出证前不计入复用/可切换集合")
            self._event_locked(
                s, "TRANSFER_STAGED",
                f"任务 {t['task_id']}（{t['job_id']}→{t['box_id']}）传输完成，"
                f"{len(staged)}/{len(indices)} 块暂存，等待盒端逐项验收",
                t["task_id"])
            return
        # fully reused: objects already proven — mint the manifest proof now
        self._journal(
            s, t, "TASK_DONE",
            "全部内容已在盒上（复用率 100%，已是已证明内容），无需传输即完成")
        self._event_locked(
            s, "TASK_DONE",
            f"任务 {t['task_id']}（{t['job_id']}→{t['box_id']}）完成（内容全部复用）",
            t["task_id"])
        self._accept_locked(s, t, basis="reused")

    def _release_staging(self, s: dict, t: dict, reason: str) -> float:
        """Physically release staged bytes (quarantine/cancel): disk shrinks."""
        n = self._staged_bytes(s, t)
        if n > EPS:
            box = s["boxes"][t["box_id"]]
            box["disk_used_bytes"] = max(0.0, box["disk_used_bytes"] - n)
            box["updated_at"] = s["clock"]
        t["staged"] = None
        if reason:
            self._journal(s, t, "STAGING_RELEASED",
                          f"{reason}：释放暂存空间 {n:.0f} 字节")
        return n

    def _clear_staging_view(self, s: dict, t: dict, reason: str) -> float:
        """Staged bytes became verified content: clear the pointer only, the
        bytes stay on disk and are accounted by box.proven_bytes."""
        n = self._staged_bytes(s, t)
        t["staged"] = None
        if reason and n > EPS:
            self._journal(s, t, "STAGING_VERIFIED",
                          f"{reason}：{n:.0f} 字节转为已证明内容（空间保留占用）")
        return n

    # ============================================ box-side acceptance core ==
    def _verify_locked(self, s: dict, man: dict, t: dict):
        """Verify the manifest object by object on the box.

        Per chunk: presence (缺块), declared block length, chunk sha256, and
        after zlib-decoding every contained object's hash and content length;
        finally the ordered chunk hashes roll up to the whole-package root
        hash (根散列). Chunks never transferred are accepted on the box's
        proven inventory (content-addressed, proven earlier).
        Returns (chunk_results, object_results, root_ok, root_observed).
        """
        staged = t.get("staged") or {}
        inv = set(s["boxes"][t["box_id"]]["inventory"])
        objs_by_chunk: dict[int, list] = {}
        for o in man["objects"]:
            for c in man["chunks"]:
                if o["object_id"] in c["objects"]:
                    objs_by_chunk.setdefault(c["index"], []).append(o)
        chunk_results, object_results, observed = {}, [], {}
        for c in man["chunks"]:
            idx, ck = c["index"], str(c["index"])
            rec = staged.get(ck)
            if rec is None:
                # not carried by this transfer: every object must be proven
                objs = objs_by_chunk.get(idx, [])
                ok = all(o["hash"] in inv for o in objs)
                chunk_results[ck] = {
                    "index": idx, "basis": "proven",
                    "declared_size": c["size"], "observed_size": None,
                    "declared_hash": c["hash"], "observed_hash": None,
                    "ok": ok,
                    "failures": [] if ok else [F_MISSING]}
                for o in objs:
                    object_results.append({
                        "object_id": o["object_id"], "chunk_index": idx,
                        "hash": o["hash"], "proven": o["hash"] in inv,
                        "basis": "proven", "failures":
                        ([] if o["hash"] in inv else [F_MISSING])})
                if ok:
                    observed[idx] = c["hash"]
                continue
            failures = []
            if rec["size"] != c["size"]:
                failures.append(F_LENGTH)
            if rec["hash"] != c["hash"]:
                failures.append(F_CHUNK_HASH)
            payload, decoded = None, None
            try:
                payload = _b64d(rec["data_b64"])
                decoded = zlib.decompress(payload)
                inner = json.loads(decoded.decode("utf-8"))
            except Exception:
                if F_CHUNK_HASH not in failures:
                    failures.append(F_DECOMPRESS)
                inner = None
            cok = not failures and inner is not None and \
                len(inner.get("objects", [])) == len(objs_by_chunk.get(idx, []))
            chunk_results[ck] = {
                "index": idx, "basis": "staged",
                "declared_size": c["size"], "observed_size": rec["size"],
                "declared_hash": c["hash"], "observed_hash": rec["hash"],
                "ok": cok, "failures": sorted(set(failures))}
            meta = {o["object_id"]: o for o in (inner or {}).get("objects", [])}
            for o in objs_by_chunk.get(idx, []):
                ofail = list(failures)
                mo = meta.get(o["object_id"])
                length = None
                if mo is None:
                    ofail.append(F_DECOMPRESS)
                else:
                    # sha256 over canonical content binds exact byte length;
                    # a mismatch means content (hence its length) is damaged
                    length = len(canonical(mo.get("content")))
                    if sha256(mo.get("content")) != o["hash"]:
                        ofail.append(F_OBJECT_HASH)
                        ofail.append(F_LENGTH)
                object_results.append({
                    "object_id": o["object_id"], "chunk_index": idx,
                    "hash": o["hash"], "proven": not ofail,
                    "basis": "staged",
                    "content_length": length,
                    "failures": sorted(set(ofail))})
            if cok:
                observed[idx] = rec["hash"]
        root_observed = root_hash_of(
            [observed.get(c["index"], c["hash"]) for c in man["chunks"]])
        root_ok = all(r["ok"] for r in chunk_results.values()) and \
            root_observed == man["root_hash"]
        return chunk_results, object_results, root_ok, root_observed

    def _accept_locked(self, s: dict, t: dict, basis: str = "verification",
                       repair_for: Optional[str] = None) -> dict:
        """Run one immutable box-side acceptance over task ``t``."""
        man = self._manifest_ref(s, t)
        box = s["boxes"][t["box_id"]]
        if man is None:
            raise DomainError("PREHEAT_MANIFEST_UNKNOWN",
                              f"清单 {t['manifest_id']} 已不可读，无法验收", 409)
        if basis == "reused" or (t["kind"] == "repair" and not t.get("staged")):
            chunk_results, object_results = {}, []
            inv = set(box["inventory"])
            for o in man["objects"]:
                object_results.append({
                    "object_id": o["object_id"], "proven": o["hash"] in inv,
                    "basis": "proven", "hash": o["hash"], "failures":
                    ([] if o["hash"] in inv else [F_MISSING])})
            root_ok, root_observed = True, man["root_hash"]
            for c in man["chunks"]:
                chunk_results[str(c["index"])] = {
                    "index": c["index"], "basis": "proven",
                    "declared_size": c["size"], "observed_size": None,
                    "declared_hash": c["hash"], "observed_hash": None,
                    "ok": all(any(r["object_id"] == oid and r["proven"]
                                  for r in object_results)
                              for oid in c["objects"]), "failures": []}
        else:
            chunk_results, object_results, root_ok, root_observed = \
                self._verify_locked(s, man, t)
        return self._record_acceptance_locked(
            s, man, t, chunk_results, object_results, root_ok, root_observed,
            basis=basis, repair_for=repair_for)

    def _record_acceptance_locked(self, s, man, t, chunk_results, object_results,
                                   root_ok, root_observed, basis, repair_for):
        box = s["boxes"][t["box_id"]]
        proven_objs = [r for r in object_results if r["proven"]]
        bad_objs = [r for r in object_results if not r["proven"]]
        reasons: dict[str, int] = {}
        for r in object_results:
            for f in r["failures"]:
                reasons[f] = reasons.get(f, 0) + 1
        if not root_ok and F_ROOT not in reasons:
            reasons[F_ROOT] = reasons.get(F_ROOT, 0) + 1
        # only proven objects become reusable content
        inv = set(box["inventory"])
        new_proven = sorted({r["hash"] for r in proven_objs} - inv)
        inv.update(new_proven)
        box["inventory"] = sorted(inv)
        full_pass = root_ok and not bad_objs
        # generations of the newest switchable proof on this box: an older
        # generation's repair may never overwrite a newer generation's proof
        gens_new = self._switchable_generations(s, t["box_id"])
        gens_man = dict(man["generations"])
        superseded = bool(gens_new) and all(
            gens_man.get(r, 0) <= g for r, g in gens_new.items()) and \
            any(gens_man.get(r, 0) < g for r, g in gens_new.items())
        s["seq"]["acceptance"] += 1
        aid = f"AC-{s['seq']['acceptance']:04d}"
        prev_digest = box.get("last_proof_digest")
        payload = {
            "manifest_id": man["manifest_id"], "manifest_hash": man["manifest_hash"],
            "generations": gens_man, "version": man["source"]["version"],
            "job_id": t["job_id"], "task_id": t["task_id"],
            "repair_for": repair_for,
            "box_id": t["box_id"], "site_id": t["site_id"],
            "box_acceptance_seq": box["acceptance_seq"] + 1,
            "basis": basis, "root_hash": man["root_hash"],
            "root_observed": root_observed, "root_ok": root_ok and not superseded,
            "objects_total": len(object_results),
            "objects_proven": len(proven_objs),
            "objects_quarantined": len(bad_objs),
            "chunk_results": chunk_results,
            "object_results": sorted(object_results, key=lambda r: r["object_id"]),
            "reasons": reasons, "parent_digest": prev_digest}
        digest = sha256(payload)
        box["acceptance_seq"] += 1
        box["last_proof_digest"] = digest
        rec = {"acceptance_id": aid, "seq": s["seq"]["acceptance"],
               "created_at": s["clock"], "kind": t["kind"],
               "valid": bool(full_pass and not superseded),
               "full_pass": bool(full_pass), "superseded": bool(superseded),
               "digest": digest, **copy.deepcopy(payload)}
        s["acceptances"][aid] = rec
        t["last_acceptance_id"] = aid

        if full_pass and not superseded:
            return self._acceptance_proven(s, t, man, box, rec, new_proven)
        if full_pass and superseded:
            return self._acceptance_superseded(s, t, man, box, rec, new_proven)
        return self._acceptance_quarantined(s, t, man, box, rec, chunk_results,
                                            bad_objs, new_proven, repair_for)

    def _switchable_generations(self, s: dict, box_id: str) -> dict:
        gens = {}
        for p in s["switchable"].get(box_id, []):
            for r, g in p["generations"].items():
                gens[r] = max(gens.get(r, 0), g)
        return gens

    def _clear_quarantine_for_manifest(self, s, box, manifest_id: str) -> int:
        """Objects repaired & re-proven leave the quarantine ledger."""
        ids = [oid for oid, q in box["quarantine"].items()
               if q["manifest_id"] == manifest_id]
        for oid in ids:
            box["quarantine"].pop(oid)
        return len(ids)

    def _acceptance_proven(self, s, t, man, box, rec, new_proven):
        t["accept_state"] = ACCEPT_PROVEN
        t["proof_id"] = rec["acceptance_id"]
        t["quarantine"] = None
        cleared = self._clear_quarantine_for_manifest(s, box, man["manifest_id"])
        if cleared:
            self._journal(s, t, "QUARANTINE_CLEARED",
                          f"修复验收通过：{cleared} 个隔离对象已恢复并出证，"
                          f"隔离台账清零（隔离容量收敛）")
        # enter the switchable set exactly once per (box, manifest)
        entries = s["switchable"].setdefault(t["box_id"], [])
        if not any(e["manifest_hash"] == man["manifest_hash"] for e in entries):
            entries.append({"manifest_id": man["manifest_id"],
                            "manifest_hash": man["manifest_hash"],
                            "version": man["source"]["version"],
                            "generations": dict(man["generations"]),
                            "root_hash": man["root_hash"],
                            "proof_id": rec["acceptance_id"],
                            "job_id": t["job_id"], "task_id": t["task_id"],
                            "proven_at": s["clock"]})
        # staged bytes just verified: count distinct content-addressed chunks
        # once, then clear only the staging pointer (bytes stay on disk)
        verified = self._count_verified_locked(s, man, t)
        self._clear_staging_view(s, t, "验收全部通过")
        repaired = t["kind"] == "repair"
        if repaired:
            self._close_repair_locked(
                s, t, {"outcome": "repaired", "acceptance_id": rec["acceptance_id"],
                       "proof_id": rec["acceptance_id"], "at": s["clock"],
                       "new_proven_objects": new_proven,
                       "new_verified_bytes": round(verified, 1)})
        self._journal(s, t, "ACCEPT_PROVEN",
                      f"盒端验收通过（{rec['objects_proven']}/{rec['objects_total']} "
                      f"对象，块长度/块散列/根散列全部一致），不可变证明 "
                      f"{rec['acceptance_id']} 已固化，内容进入复用与可切换集合")
        self._event_locked(
            s, "ACCEPT_PROVEN",
            f"{t['box_id']} 验收通过：{man['manifest_id']}（{man['source']['version']}，"
            f"代次 {man['generations']}）出证 {rec['acceptance_id']}，根散列 "
            f"{short(man['root_hash'])}" +
            (f"，修复 {t['repair_id']} 收敛" if repaired else ""),
            rec["acceptance_id"])
        return rec

    def _count_verified_locked(self, s, man, t) -> float:
        """Add staged-and-verified chunk hashes to the box's verified set;
        return the bytes newly attributable to valid proofs."""
        box = s["boxes"][t["box_id"]]
        verified = box.setdefault("verified_chunks", {})
        added = 0.0
        for c in man["chunks"]:
            if str(c["index"]) in (t.get("staged") or {}) \
                    and c["hash"] not in verified:
                verified[c["hash"]] = c["size"]
                added += c["size"]
        box["proven_bytes"] = round(sum(verified.values()), 1)
        box["updated_at"] = s["clock"]
        return added

    def _acceptance_superseded(self, s, t, man, box, rec, new_proven):
        # old-generation repair finished: proof is immutable but NOT switchable.
        # the bytes themselves are hash-verified content: they stay on disk
        # (counted once as verified, reusable by hash) but never flip pointers.
        t["accept_state"] = ACCEPT_SUPERSEDED
        t["proof_id"] = rec["acceptance_id"]
        self._count_verified_locked(s, man, t)
        self._clear_staging_view(s, t, "旧代次修复不得覆盖新代次证明")
        if t["kind"] == "repair":
            self._close_repair_locked(
                s, t, {"outcome": "superseded",
                        "acceptance_id": rec["acceptance_id"], "at": s["clock"],
                        "new_proven_objects": new_proven},
                result_outcome="superseded")
        self._journal(
            s, t, "ACCEPT_SUPERSEDED",
            f"修复内容本身验真通过，但清单代次 {man['generations']} 落后于盒上"
            f"可切换集合的更新代次 {self._switchable_generations(s, t['box_id'])}；"
            f"证明 {rec['acceptance_id']} 固化为不可切换（valid=false），"
            f"绝不覆盖新代次证明")
        self._event_locked(s, "ACCEPT_SUPERSEDED",
                           f"{t['box_id']} 旧代次修复 {t.get('repair_id')} 收敛："
                           f"证明 {rec['acceptance_id']} 不可切换，新代次证明保持不动",
                           rec["acceptance_id"])
        return rec

    def _acceptance_quarantined(self, s, t, man, box, rec, chunk_results,
                                 bad_objs, new_proven, repair_for):
        """Isolate ONLY affected objects, release their space, create one
        repair over the remaining blocks."""
        bad_chunks = sorted({r["chunk_index"] for r in bad_objs})
        affected = sorted({r["object_id"] for r in bad_objs})
        # drop precisely the bad chunks from staging → release their bytes
        keep = {k: v for k, v in (t.get("staged") or {}).items()
                if int(k) not in bad_chunks}
        drop = {k: v for k, v in (t.get("staged") or {}).items()
                if int(k) in bad_chunks}
        released = sum(len(_b64d(v["data_b64"])) for v in drop.values())
        if released > EPS:
            box["disk_used_bytes"] = max(0.0, box["disk_used_bytes"] - released)
        t["staged"] = keep or None
        # quarantine ledger per affected object (capacity attribution)
        obj_size = {o["object_id"]: 0 for o in man["objects"]}
        for c in man["chunks"]:
            share = c["size"] / max(1, len(c["objects"]))
            for oid in c["objects"]:
                obj_size[oid] = obj_size.get(oid, 0) + share
        for r in bad_objs:
            oid = r["object_id"]
            box["quarantine"][oid] = {
                "object_id": oid, "hash": r["hash"],
                "manifest_id": man["manifest_id"],
                "job_id": t["job_id"], "task_id": t["task_id"],
                "size_bytes": round(obj_size.get(oid, 0), 1),
                "chunk_index": r["chunk_index"],
                "reasons": r["failures"],
                "acceptance_id": rec["acceptance_id"], "since": s["clock"]}
        t["accept_state"] = ACCEPT_QUARANTINED
        self._journal(
            s, t, "OBJECTS_QUARANTINED",
            f"验收发现 {len(affected)} 个受影响对象（块 {bad_chunks}）：仅隔离这些"
            f"对象并释放 {released:.0f} 字节；{len(new_proven)} 个已证明对象不重传，"
            f"根散列{'不一致，' if not rec['root_ok'] else ''}原因 {rec['reasons']}")
        self._event_locked(
            s, "OBJECTS_QUARANTINED",
            f"{t['box_id']} 验收 {man['manifest_id']}：隔离 {len(affected)} 对象，"
            f"释放 {released:.0f} B，失败原因 {rec['reasons']}；"
            f"已证明 {rec['objects_proven']} 对象保留复用",
            rec["acceptance_id"])
        # one repair over the remaining (bad) blocks, always off the ORIGINAL
        # primary transfer task (flat chain: a failed repair closes and the
        # next attempt re-anchors to the same primary)
        primary = t if t["kind"] == "transfer" \
            else s["tasks"][t["root_task_id"]]
        repair = self._create_repair_locked(
            s, man, primary, bad_chunks, rec["acceptance_id"],
            affected, released, rec["reasons"])
        rec["repair_id"] = repair["repair_id"]
        if t["kind"] == "repair":
            # healthy bytes merged in for verification belong to the primary;
            # clear only this attempt's view, then open the follow-up repair
            t["staged"] = None
            self._close_repair_locked(
                s, t, {"outcome": "requarantined",
                        "acceptance_id": rec["acceptance_id"],
                        "next_repair_id": repair["repair_id"],
                        "released_bytes": round(released, 1),
                        "at": s["clock"]}, journal="REPAIR_REQUARANTINED")
        return rec

    # ============================================================ repairs ==
    def _create_repair_locked(self, s, man, parent, chunk_indices, acceptance_id,
                               affected, released_bytes, reasons):
        """Exactly one repair per (parent task, still-missing block set).

        Key = (parent task, manifest, sorted chunks): concurrent submissions
        return the first repair. A canceled repair releases its key so a later
        retry deterministically creates a new repair (attempt suffix).
        """
        base_key = sha256({"parent": parent["task_id"],
                           "manifest_hash": man["manifest_hash"],
                           "chunks": sorted(chunk_indices)})
        # the active attempt is the highest suffix not yet created; a canceled
        # attempt keeps its key, so a retry deterministically takes the next one
        attempt = parent.get("repair_attempts", 0)
        while f"{base_key}:{attempt}" in s["repair_by_key"] and \
                s["repairs"][s["repair_by_key"][f"{base_key}:{attempt}"]]["status"] \
                in ("queued", "transferring", "paused", "done"):
            attempt += 1
        key = f"{base_key}:{attempt}"
        rid0 = s["repair_by_key"].get(key)
        if rid0:
            return s["repairs"][rid0]
        s["seq"]["repair"] += 1
        rid = f"RX-{s['seq']['repair']:04d}"
        tid = self._next(s, "task", "PT")
        total = sum(c["size"] for c in man["chunks"]
                    if c["index"] in chunk_indices)
        rt = {"task_id": tid, "seq": s["seq"]["task"], "job_id": parent["job_id"],
              "kind": "repair", "box_id": parent["box_id"],
              "site_id": parent["site_id"], "status": "queued",
              "manifest_id": man["manifest_id"],
              "manifest_hash": man["manifest_hash"],
              "accept_state": ACCEPT_REPAIRING,
              "staged": None, "damage": None, "proof_id": None,
              "last_acceptance_id": None,
              "parent_task_id": parent["task_id"],
              "root_task_id": parent["task_id"],
              "repair_id": rid,
              "repair_result": None, "chunk_indices": sorted(chunk_indices),
              "bytes_total": total, "bytes_done": 0.0,
              "reserved_bytes": 0.0, "debt_kb": 0.0,
              "reuse_ratio": 0.0, "journal": [],
              "created_at": s["clock"], "updated_at": s["clock"],
              "done_at": None}
        repair = {"repair_id": rid, "task_id": tid, "seq": s["seq"]["repair"],
                  "repair_key": key, "attempt": attempt,
                  "job_id": parent["job_id"], "box_id": parent["box_id"],
                  "site_id": parent["site_id"],
                  "manifest_id": man["manifest_id"],
                  "manifest_hash": man["manifest_hash"],
                  "parent_task_id": parent["task_id"],
                  "source_acceptance_id": acceptance_id,
                  "chunk_indices": sorted(chunk_indices),
                  "affected_objects": affected,
                  "released_bytes": round(released_bytes, 1),
                  "reasons": reasons,
                  "status": "queued", "result": None,
                  "created_at": s["clock"], "updated_at": s["clock"]}
        s["tasks"][tid] = rt
        s["repairs"][rid] = repair
        s["repair_by_key"][key] = rid
        parent["repair_id"] = rid
        parent["repair_attempts"] = attempt + 1
        parent["accept_state"] = ACCEPT_REPAIRING
        parent["updated_at"] = s["clock"]
        self._journal(s, rt, "REPAIR_CREATED",
                      f"按验收 {acceptance_id} 创建一次修复：仅重传剩余块 "
                      f"{sorted(chunk_indices)}（{total:.0f} 字节）；"
                      f"已证明对象不重传，并发同键返回本任务")
        self._event_locked(
            s, "REPAIR_CREATED",
            f"{parent['box_id']} 创建修复 {rid}（父任务 {parent['task_id']}，"
            f"{len(chunk_indices)} 块 / {len(affected)} 隔离对象）", rid)
        return repair

    def _close_repair_locked(self, s, rt: dict, result: dict,
                              journal: str = "REPAIR_CLOSED",
                              result_outcome: Optional[str] = None):
        rid = rt["repair_id"]
        repair = s["repairs"][rid]
        result = copy.deepcopy(result)
        result.setdefault("at", s["clock"])
        # NOTE: the repair task's staged bytes were merged from the parent for
        # verification; the parent owns that disk. Callers move/keep the
        # staging before closing, so here we only clear the repair's view.
        if rt["status"] != "canceled":
            rt["status"] = "done" if result.get("outcome") in (
                "repaired", "superseded", "requarantined") else "canceled"
        rt["staged"] = None
        rt["reserved_bytes"] = 0.0
        rt["repair_result"] = result
        rt["updated_at"] = s["clock"]
        repair["status"] = rt["status"]
        repair["result"] = result
        repair["updated_at"] = s["clock"]
        parent = s["tasks"].get(rt["parent_task_id"])
        if parent is not None and result.get("outcome") in (
                "repaired", "superseded"):
            parent["repair_result"] = result
        self._journal(s, rt, journal,
                      f"修复 {rid} 收敛：{result.get('outcome')}（确定结果，"
                      f"磁盘预留与传输名额成对释放，无残留占用）")
        self._event_locked(
            s, {"repaired": "REPAIR_CLOSED", "superseded": "REPAIR_CLOSED",
                "requarantined": "REPAIR_REQUARANTINED",
                "canceled": "REPAIR_CANCELED"}.get(
                    result.get("outcome"), "REPAIR_CLOSED"),
            f"修复 {rid}（{rt['box_id']}）结果：{result.get('outcome')}", rid)

    def cancel_repair(self, repair_id: str, by: str = "dispatcher") -> dict:
        """Cancel a repair: pair released, quarantine stays, key is freed so
        the next request creates a fresh repair (verdict-once per attempt)."""
        with self.lock:
            repair = self.state["repairs"].get(repair_id)
            if repair is None:
                raise DomainError("PREHEAT_REPAIR_UNKNOWN",
                                  f"修复 {repair_id} 不存在", 404)
            if repair["status"] not in ("queued", "transferring", "paused"):
                return {"repair_id": repair_id, "status": repair["status"],
                        "result": copy.deepcopy(repair["result"]),
                        "idempotent": True}

        def m(s):
            rp = s["repairs"][repair_id]
            rt = s["tasks"][rp["task_id"]]
            released = self._staged_bytes(s, rt) + rt["reserved_bytes"]
            self._release_staging(s, rt, "修复取消")
            rt["status"] = "canceled"
            rt["reserved_bytes"] = 0.0
            parent = s["tasks"][rt["parent_task_id"]]
            # parent returns to quarantine; its healthy chunks stay on disk
            parent["accept_state"] = ACCEPT_QUARANTINED
            parent["repair_id"] = None
            self._close_repair_locked(
                s, rt, {"outcome": "canceled", "by": by, "at": s["clock"],
                        "released_bytes": round(released, 1)})
            return copy.deepcopy(rp["result"])

        result = self.commit(m)
        return {"repair_id": repair_id, "status": "canceled",
                "result": result, "idempotent": False}

    # ====================================================== public accept ==
    def accept_task(self, task_id: str, body: Optional[dict] = None) -> dict:
        """Box-side acceptance: verify length / chunk hash / root hash and
        mint immutable proof, or isolate affected objects and repair once."""
        body = body or {}
        with self.lock:
            t = self.state["tasks"].get(task_id)
            if t is None:
                raise DomainError("PREHEAT_TASK_UNKNOWN",
                                  f"任务 {task_id} 不存在", 404)
            if t["kind"] == "repair":
                rp = self.state["repairs"].get(t["repair_id"])
                if rp and rp["status"] not in ("queued", "transferring", "paused"):
                    return {"acceptance_id": t.get("last_acceptance_id"),
                            "task_id": task_id, "idempotent": True,
                            "repair": self._repair_view(self.state, rp)}
            if t["accept_state"] in ACCEPT_CLOSED and t["status"] == "done":
                return {"acceptance_id": t["last_acceptance_id"],
                        "task_id": task_id, "idempotent": True,
                        "acceptance": copy.deepcopy(
                            self.state["acceptances"][t["last_acceptance_id"]])}
            if t["status"] != "done":
                raise DomainError(
                    "PREHEAT_NOT_STAGED",
                    f"任务 {task_id} 状态为 {t['status']}：作业传输尚未完成，"
                    f"无法盒端验收", 409)
            if t["accept_state"] == ACCEPT_REPAIRING:
                raise DomainError(
                    "PREHEAT_REPAIR_OPEN",
                    f"任务 {task_id} 修复进行中：请对修复 "
                    f"{t.get('repair_id')} 验收，或取消修复后重试", 409)
            # quarantined primary (e.g. after a canceled repair): re-running
            # acceptance re-creates ONE repair with the same deterministic key;
            # a canceled attempt bumps the attempt suffix deterministically.
            if t["accept_state"] == ACCEPT_QUARANTINED and t["kind"] == "transfer":
                re_repair = True
            else:
                re_repair = False

        def m(s):
            tt = s["tasks"][task_id]
            self._apply_damage_locked(s, tt, body.get("damage") or tt.get("damage"))
            if tt["kind"] == "transfer" and tt["accept_state"] == ACCEPT_QUARANTINED:
                # re-run box-side acceptance after a canceled/failed repair:
                # the still-missing blocks are isolated once more and exactly
                # one fresh repair is created (deterministic key)
                man = self._manifest_ref(s, tt)
                chunk_results, object_results, root_ok, root_observed = \
                    self._verify_locked(s, man, tt)
                rec = self._record_acceptance_locked(
                    s, man, tt, chunk_results, object_results, root_ok,
                    root_observed, basis="re-acceptance", repair_for=None)
                out = {"acceptance_id": rec["acceptance_id"],
                       "acceptance": copy.deepcopy(rec), "idempotent": False}
                if rec.get("repair_id"):
                    out["repair"] = self._repair_view(
                        s, s["repairs"][rec["repair_id"]])
                return out
            if tt["kind"] == "repair":
                # merge healthy chunks still staged by the original transfer:
                # they were hash-verified object-wise at the first acceptance
                # and must never be retransferred/rejudged missing
                primary = s["tasks"][tt["root_task_id"]]
                merged = dict(primary.get("staged") or {})
                merged.update(tt.get("staged") or {})
                tt["staged"] = merged or None
                rec = self._accept_locked(
                    s, tt, basis="repair", repair_for=primary["task_id"])
                if rec["full_pass"]:
                    # verified bytes (healthy originals + repaired) are handed
                    # back to the manifest-owning primary; repair holds none.
                    # an older-generation repair is proven-immutable but not
                    # switchable: it must never overwrite the newer proof.
                    primary["staged"] = copy.deepcopy(tt["staged"])
                    primary["accept_state"] = (ACCEPT_SUPERSEDED
                                               if rec["superseded"]
                                               else ACCEPT_PROVEN)
                    primary["proof_id"] = rec["acceptance_id"]
                    primary["last_acceptance_id"] = rec["acceptance_id"]
                    primary["repair_id"] = None
                    primary["updated_at"] = s["clock"]
                rp = s["repairs"][tt["repair_id"]]
                return {"acceptance_id": rec["acceptance_id"],
                        "acceptance": copy.deepcopy(rec),
                        "repair": self._repair_view(s, rp),
                        "parent_task_id": primary["task_id"],
                        "idempotent": False}
            rec = self._accept_locked(s, tt)
            out = {"acceptance_id": rec["acceptance_id"],
                   "acceptance": copy.deepcopy(rec), "idempotent": False}
            if rec.get("repair_id"):
                out["repair"] = self._repair_view(s, s["repairs"][rec["repair_id"]])
            return out

        return self.commit(m)

    def inject_damage(self, task_id: str, body: dict) -> dict:
        """Fault injection for the next acceptance: drop chunks or corrupt
        bytes (缺块 / 块损坏). Only before acceptance verdict."""
        missing = body.get("missing_chunks") or []
        corrupt = body.get("corrupt_chunks") or []
        try:
            missing = [int(x) for x in missing]
            corrupt = [int(x) for x in corrupt]
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "块号必须是整数数组", 400)

        with self.lock:
            t = self.state["tasks"].get(task_id)
            if t is None:
                raise DomainError("PREHEAT_TASK_UNKNOWN",
                                  f"任务 {task_id} 不存在", 404)

        def m(s):
            tt = s["tasks"][task_id]
            if tt["status"] != "done":
                # transfer still running: remember for staging at completion
                tt["damage"] = {"missing": missing, "corrupt": corrupt}
                return {"task_id": task_id, "armed": True,
                        "missing_chunks": missing, "corrupt_chunks": corrupt}
            if tt["accept_state"] in ACCEPT_CLOSED:
                raise DomainError("PREHEAT_TASK_CLOSED",
                                  f"任务 {task_id} 已出证（判定一次终身），不能再注入损坏",
                                  409)
            if tt["accept_state"] == ACCEPT_REPAIRING:
                raise DomainError("PREHEAT_REPAIR_OPEN",
                                  f"任务 {task_id} 修复中，请对修复任务注入", 409)
            self._apply_damage_locked(s, tt, {"missing": missing, "corrupt": corrupt})
            return {"task_id": task_id, "armed": False,
                    "missing_chunks": missing, "corrupt_chunks": corrupt,
                    "staged_chunks": sorted(int(k) for k in (tt["staged"] or {}))}

        return self.commit(m)

    def _apply_damage_locked(self, s, t, damage):
        if not damage or not t.get("staged"):
            return
        missing = [int(x) for x in damage.get("missing", [])]
        corrupt = [int(x) for x in damage.get("corrupt", [])]
        for idx in missing:
            t["staged"].pop(str(idx), None)
        for idx in corrupt:
            rec = t["staged"].get(str(idx))
            if rec is None:
                continue
            raw = _b64d(rec["data_b64"])
            raw = raw[:-1] + bytes([raw[-1] ^ 0xFF])
            rec["data_b64"] = base64.b64encode(raw).decode("ascii")
            rec["size"] = len(raw)
            rec["hash"] = sha256_bytes(raw)
        if missing or corrupt:
            self._journal(
                s, t, "DAMAGE_ARMED",
                f"盒端故障注入：缺块 {sorted(set(missing))}，损坏块 "
                f"{sorted(set(corrupt))}（仅影响本次验收）")
        t["damage"] = None

    # ================================================================ tick ==
    def tick(self, seconds: float) -> dict:
        """Advance the simulated clock; transfers progress at granted rates.

        Debt (欠账) accrues for every eligible-but-underserved task, so a
        continuously progressing job can never starve; jobs short of their
        minimum online guarantee accrue double.
        """
        try:
            secs = float(seconds)
        except (TypeError, ValueError):
            raise DomainError("BAD_REQUEST", "tick 秒数必须是数字", 400)
        if secs <= 0 or secs > 86400:
            raise DomainError("BAD_REQUEST", "tick 秒数须在 (0, 86400] 内", 400)

        def m(s):
            plan = self._plan(s)
            s["clock"] = _iso(self._clock(s) + timedelta(seconds=secs))
            # debt accrual per site (fair share = min(want, capacity / contenders))
            by_site: dict[str, list] = {}
            for t in self._open_tasks(s):
                by_site.setdefault(t["site_id"], []).append(t)
            for site_id, tasks in by_site.items():
                site = s["sites"].get(site_id)
                if site is None:
                    continue
                n = len(tasks)
                for t in tasks:
                    job = s["jobs"][t["job_id"]]
                    fair = min(self._want(s, t), site["capacity_kbps"] / n)
                    grant = plan.get(t["task_id"], {}).get("grant_kbps", 0.0)
                    done_n = sum(1 for tid in job["tasks"]
                                 if s["tasks"][tid]["status"] == "done")
                    boost = 2.0 if done_n < job["min_online"] else 1.0
                    t["debt_kb"] = _round6(
                        t["debt_kb"] + (fair - grant) * secs * boost)
            # advance transfers at the granted rate
            progressed, completed = [], []
            for t in self._open_tasks(s):
                p = plan.get(t["task_id"])
                if not p or p["grant_kbps"] <= EPS or t["status"] != "transferring":
                    continue
                add = p["grant_kbps"] * KB * secs
                t["bytes_done"] = min(t["bytes_total"], t["bytes_done"] + add)
                t["updated_at"] = s["clock"]
                progressed.append({"task_id": t["task_id"],
                                   "bytes_done": round(t["bytes_done"], 1),
                                   "bytes_total": round(t["bytes_total"], 1)})
                if self._remaining(t) <= EPS:
                    self._complete_task(s, t)
                    completed.append(t["task_id"])
            return {"clock": s["clock"], "advanced": progressed,
                    "completed": completed}

        return self.commit(m)

    # ================================================================ views ==
    def _predict(self, s: dict) -> dict:
        """Deterministic ETA simulation: {task_id: seconds_from_now | None}.

        Replays the pure plan forward (debts frozen, current eligibility),
        finishing tasks one by one until nothing can progress.
        """
        remaining = {t["task_id"]: self._remaining(t)
                     for t in self._open_tasks(s) if self._remaining(t) > EPS}
        eta: dict[str, Optional[float]] = {}
        horizon = 0.0
        for _ in range(10000):
            if not remaining:
                break
            plan = self._plan(s, remaining)
            active = {tid: p["grant_kbps"] for tid, p in plan.items()
                      if tid in remaining and p["grant_kbps"] > EPS}
            if not active:
                break
            dt = min(remaining[tid] / (g * KB) for tid, g in active.items())
            horizon += dt
            for tid, g in active.items():
                remaining[tid] -= g * KB * dt
                if remaining[tid] <= EPS:
                    eta[tid] = horizon
                    del remaining[tid]
        for tid in [t["task_id"] for t in self._open_tasks(s)]:
            eta.setdefault(tid, None)
        return eta

    def _views(self, s: dict):
        """Compute plan + ETA + ranks once, consistently for all views."""
        plan = self._plan(s)
        eta = self._predict(s)
        ranks = {}
        for i, t in enumerate(sorted(self._open_tasks(s), key=self._priority(s))):
            ranks[t["task_id"]] = i + 1
        return plan, eta, ranks

    def _task_view(self, s: dict, t: dict, plan=None, eta=None, ranks=None) -> dict:
        job = s["jobs"][t["job_id"]]
        v = {"task_id": t["task_id"], "job_id": t["job_id"], "box_id": t["box_id"],
             "site_id": t["site_id"], "status": t["status"],
             "kind": t.get("kind", "transfer"),
             "manifest_id": t.get("manifest_id"),
             "accept_state": t.get("accept_state", ACCEPT_PENDING),
             "accept_state_cn": ACCEPT_STATE_CN.get(
                 t.get("accept_state", ACCEPT_PENDING), t.get("accept_state", "")),
             "proof_id": t.get("proof_id"),
             "last_acceptance_id": t.get("last_acceptance_id"),
             "parent_task_id": t.get("parent_task_id"),
             "repair_id": t.get("repair_id"),
             "repair_result": copy.deepcopy(t.get("repair_result")),
             "staged_bytes": round(self._staged_bytes(s, t), 1),
             "staged_chunks": sorted(int(k) for k in (t.get("staged") or {})),
             "bytes_total": round(t["bytes_total"], 1),
             "bytes_done": round(t["bytes_done"], 1),
             "remaining_bytes": round(self._remaining(t), 1),
             "reserved_bytes": round(t["reserved_bytes"], 1),
             "debt_kb": round(t["debt_kb"], 3),
             "reuse_ratio": round(t["reuse_ratio"], 4),
             "progress": (round(t["bytes_done"] / t["bytes_total"], 4)
                          if t["bytes_total"] > EPS else 1.0),
             "journal": copy.deepcopy(t["journal"]),
             "created_at": t["created_at"], "updated_at": t["updated_at"],
             "done_at": t["done_at"]}
        last = None
        if t.get("last_acceptance_id"):
            last = s["acceptances"].get(t["last_acceptance_id"])
        if last:
            v["last_acceptance"] = {
                "acceptance_id": last["acceptance_id"],
                "valid": last["valid"], "full_pass": last["full_pass"],
                "superseded": last["superseded"],
                "objects_total": last["objects_total"],
                "objects_proven": last["objects_proven"],
                "objects_quarantined": last["objects_quarantined"],
                "reasons": copy.deepcopy(last["reasons"]),
                "pass_rate": (round(last["objects_proven"] /
                                    max(1, last["objects_total"]), 4)),
                "created_at": last["created_at"], "digest": last["digest"]}
        p = (plan or {}).get(t["task_id"])
        if t["status"] == "paused":
            v["rate_kbps"] = 0.0
            v["limit"] = {"code": "PAUSED", "message": REASONS["PAUSED"]}
        elif t["status"] in TASK_OPEN and p is not None:
            # transferring ⇔ holds the (disk, slot) pair ⇔ granted rate > 0
            v["rate_kbps"] = p["grant_kbps"] if t["status"] == "transferring" else 0.0
            v["limit"] = {"code": p["code"], "message": p["message"]}
        else:
            v["rate_kbps"] = 0.0
            v["limit"] = None
        if ranks is not None and t["task_id"] in ranks:
            v["rank"] = ranks[t["task_id"]]
        secs = (eta or {}).get(t["task_id"])
        if t["status"] == "done":
            v["eta_seconds"] = 0.0
            v["eta"] = t["done_at"]
        elif secs is None:
            v["eta_seconds"] = None
            v["eta"] = None
        else:
            v["eta_seconds"] = _round6(secs)
            v["eta"] = _iso(self._clock(s) + timedelta(seconds=secs))
        if t["status"] in TASK_OPEN + ("paused",):
            v["at_risk"] = bool(v["eta"] and v["eta"] > job["deadline"])
        else:
            v["at_risk"] = False
        return v

    def _repair_view(self, s: dict, r: dict) -> dict:
        v = copy.deepcopy(r)
        v["reasons_cn"] = {k: ACCEPT_REASONS.get(k, k)
                           for k in (r.get("reasons") or {})}
        return v

    def _job_view(self, s: dict, job: dict, idempotent: bool = False) -> dict:
        plan, eta, ranks = self._views(s)
        tasks = [self._task_view(s, s["tasks"][tid], plan, eta, ranks)
                 for tid in job.get("tasks", [])]
        done = sum(1 for t in tasks if self._primary_closed(
            s, s["tasks"][t["task_id"]]))
        proven = sum(1 for t in tasks
                     if t["accept_state"] in (ACCEPT_PROVEN, ACCEPT_SUPERSEDED))
        pending_accept = sum(1 for t in tasks
                             if t["status"] == "done"
                             and t["accept_state"] == ACCEPT_PENDING)
        quarantined = sum(1 for t in tasks
                          if t["accept_state"] in (ACCEPT_QUARANTINED,
                                                   ACCEPT_REPAIRING))
        repairs = [self._repair_view(s, r)
                   for r in s["repairs"].values() if r["job_id"] == job["job_id"]]
        total_b = sum(t["bytes_total"] for t in tasks)
        done_b = sum(t["bytes_done"] for t in tasks)
        by_deadline = proven + sum(
            1 for t in tasks if t["status"] in TASK_OPEN + ("paused",)
            and t["eta"] is not None and t["eta"] <= job["deadline"])
        v = {"job_id": job["job_id"], "manifest_id": job["manifest_id"],
             "version": job["version"], "targets": list(job["targets"]),
             "deadline": job["deadline"], "link_cap_kbps": job["link_cap_kbps"],
             "min_online": job["min_online"], "note": job["note"],
             "status": job["status"], "created_by": job["created_by"],
             "created_at": job["created_at"], "updated_at": job["updated_at"],
             "progress": {"done_tasks": done, "total_tasks": len(tasks),
                          "proven_tasks": proven,
                          "pending_accept_tasks": pending_accept,
                          "quarantined_tasks": quarantined,
                          "repairs": len(repairs),
                          "done_bytes": round(done_b, 1),
                          "total_bytes": round(total_b, 1)},
             "guarantee": {"required": job["min_online"], "done": proven,
                           "predicted_by_deadline": by_deadline,
                           "at_risk": by_deadline < job["min_online"]},
             "tasks": tasks, "repairs": repairs, "idempotent": idempotent}
        if job["cancel_result"]:
            v["cancel_result"] = copy.deepcopy(job["cancel_result"])
        return v

    def queue(self) -> dict:
        """The scheduling queue: identical order/reasons/ETAs for web and CLI."""
        with self.lock:
            s = self.state
            plan, eta, ranks = self._views(s)
            open_tasks = sorted(self._open_tasks(s), key=self._priority(s))
            resting = sorted(
                (t for t in s["tasks"].values()
                 if t["status"] in ("paused", "done", "canceled")),
                key=lambda t: t["seq"], reverse=True)
            site_views = []
            for site in sorted(s["sites"].values(), key=lambda x: x["site_id"]):
                tids = [t["task_id"] for t in self._open_tasks(s)
                        if t["site_id"] == site["site_id"]]
                used = sum(plan.get(tid, {}).get("grant_kbps", 0.0) for tid in tids)
                site_views.append({
                    "site_id": site["site_id"],
                    "capacity_kbps": site["capacity_kbps"],
                    "granted_kbps": _round6(used),
                    "free_kbps": _round6(site["capacity_kbps"] - used),
                    "open_tasks": len(tids)})
            return {"clock": s["clock"],
                    "sites": site_views,
                    "queue": [self._task_view(s, t, plan, eta, ranks)
                              for t in open_tasks],
                    "inactive": [self._task_view(s, t, plan, eta, ranks)
                                 for t in resting]}

    def list_jobs(self) -> list:
        with self.lock:
            return [self._job_view(self.state, j)
                    for j in sorted(self.state["jobs"].values(),
                                    key=lambda j: j["seq"], reverse=True)]

    def get_job(self, job_id: str) -> dict:
        with self.lock:
            return self._job_view(self.state, self._get_job(self.state, job_id))

    def list_sites(self) -> list:
        with self.lock:
            plan = self._plan(self.state)
            out = []
            for site in sorted(self.state["sites"].values(),
                               key=lambda x: x["site_id"]):
                tids = [t["task_id"] for t in self._open_tasks(self.state)
                        if t["site_id"] == site["site_id"]]
                used = sum(plan.get(tid, {}).get("grant_kbps", 0.0) for tid in tids)
                boxes = [b for b in self.state["boxes"].values()
                         if b["site_id"] == site["site_id"]]
                out.append({"site_id": site["site_id"],
                            "capacity_kbps": site["capacity_kbps"],
                            "granted_kbps": _round6(used),
                            "boxes": len(boxes),
                            "online": sum(1 for b in boxes if b["online"]),
                            "created_at": site["created_at"]})
            return out

    def list_boxes(self) -> list:
        with self.lock:
            return [self._box_view(self.state, b)
                    for b in sorted(self.state["boxes"].values(),
                                    key=lambda b: b["box_id"])]

    def get_site(self, site_id: str) -> dict:
        with self.lock:
            site = self.state["sites"].get(site_id)
            if not site:
                raise DomainError("PREHEAT_SITE_UNKNOWN",
                                  f"站点 {site_id} 未注册", 404)
            return next(x for x in self.list_sites() if x["site_id"] == site_id)

    def get_box(self, box_id: str) -> dict:
        with self.lock:
            box = self.state["boxes"].get(box_id)
            if not box:
                raise DomainError("PREHEAT_BOX_UNKNOWN",
                                  f"盒子 {box_id} 未注册", 404)
            return self._box_view(self.state, box)

    # ============================================= acceptance/repair views ==
    def list_acceptances(self, box_id: Optional[str] = None,
                         manifest_id: Optional[str] = None,
                         job_id: Optional[str] = None, limit: int = 200) -> list:
        with self.lock:
            out = []
            for a in self.state["acceptances"].values():
                if box_id and a["box_id"] != box_id:
                    continue
                if manifest_id and a["manifest_id"] != manifest_id:
                    continue
                if job_id and a["job_id"] != job_id:
                    continue
                out.append(self._acceptance_view(self.state, a))
            out.sort(key=lambda a: a["seq"], reverse=True)
            return out[:limit]

    def get_acceptance(self, acceptance_id: str) -> dict:
        with self.lock:
            a = self.state["acceptances"].get(acceptance_id)
            if not a:
                raise DomainError("PREHEAT_ACCEPTANCE_UNKNOWN",
                                  f"验收 {acceptance_id} 不存在", 404)
            return self._acceptance_view(self.state, a)

    def _acceptance_view(self, s: dict, a: dict) -> dict:
        return copy.deepcopy(a)

    def list_repairs(self, box_id: Optional[str] = None,
                     job_id: Optional[str] = None) -> list:
        with self.lock:
            s = self.state
            out = []
            for r in s["repairs"].values():
                if box_id and r["box_id"] != box_id:
                    continue
                if job_id and r["job_id"] != job_id:
                    continue
                out.append(self._repair_view(s, r))
            return sorted(out, key=lambda r: r["seq"], reverse=True)

    def get_repair(self, repair_id: str) -> dict:
        with self.lock:
            s = self.state
            r = s["repairs"].get(repair_id)
            if not r:
                raise DomainError("PREHEAT_REPAIR_UNKNOWN",
                                  f"修复 {repair_id} 不存在", 404)
            v = self._repair_view(s, r)
            v["task"] = self._task_view(s, s["tasks"][r["task_id"]])
            return v

    def list_switchable(self, box_id: Optional[str] = None) -> dict:
        with self.lock:
            out = {}
            for bid, entries in self.state["switchable"].items():
                if box_id and bid != box_id:
                    continue
                out[bid] = copy.deepcopy(entries)
            return {"switchable": out}

    def acceptance_summary(self, group_by: str = "site",
                           site_id: Optional[str] = None,
                           manifest_id: Optional[str] = None) -> dict:
        """Batch query: pass rate, quarantined capacity and failure reasons,
        rolled up by site or manifest. Every verdict stays traceable via the
        acceptance/repair journals."""
        group_by = group_by if group_by in ("site", "manifest") else "site"
        with self.lock:
            s = self.state
            groups: dict[str, dict] = {}
            latest: dict[tuple, str] = {}
            # count every box-manifest at its LAST acceptance (确定结果)
            for a in s["acceptances"].values():
                latest[(a["box_id"], a["manifest_id"])] = a["acceptance_id"]
            for a in s["acceptances"].values():
                if latest[(a["box_id"], a["manifest_id"])] != a["acceptance_id"]:
                    continue
                if site_id and a["site_id"] != site_id:
                    continue
                if manifest_id and a["manifest_id"] != manifest_id:
                    continue
                gk = a["site_id"] if group_by == "site" else a["manifest_id"]
                g = groups.setdefault(gk, {
                    "key": gk, "acceptances": 0, "proven": 0,
                    "quarantined": 0, "superseded": 0,
                    "objects_total": 0, "objects_proven": 0,
                    "objects_quarantined": 0,
                    "quarantine_bytes": 0.0, "pass_rate": 0.0,
                    "reasons": {}, "manifests": set(), "boxes": set()})
                g["acceptances"] += 1
                g["manifests"].add(a["manifest_id"])
                g["boxes"].add(a["box_id"])
                g["objects_total"] += a["objects_total"]
                g["objects_proven"] += a["objects_proven"]
                g["objects_quarantined"] += a["objects_quarantined"]
                if a["valid"]:
                    g["proven"] += 1
                elif a["superseded"]:
                    g["superseded"] += 1
                else:
                    g["quarantined"] += 1
                for code, n in (a.get("reasons") or {}).items():
                    g["reasons"][code] = g["reasons"].get(code, 0) + n
            # quarantined capacity from the live quarantine ledger
            for bid, box in s["boxes"].items():
                sid = box["site_id"]
                for q in box["quarantine"].values():
                    if site_id and sid != site_id:
                        continue
                    if manifest_id and q["manifest_id"] != manifest_id:
                        continue
                    gk = sid if group_by == "site" else q["manifest_id"]
                    g = groups.setdefault(gk, {
                        "key": gk, "acceptances": 0, "proven": 0,
                        "quarantined": 0, "superseded": 0,
                        "objects_total": 0, "objects_proven": 0,
                        "objects_quarantined": 0,
                        "quarantine_bytes": 0.0, "pass_rate": 0.0,
                        "reasons": {}, "manifests": set(), "boxes": set()})
                    g["quarantine_bytes"] += q["size_bytes"]
            rows = []
            for g in groups.values():
                g["quarantine_bytes"] = round(g["quarantine_bytes"], 1)
                g["pass_rate"] = (round(g["objects_proven"]
                                        / max(1, g["objects_total"]), 4))
                g["manifests"] = sorted(g["manifests"])
                g["boxes"] = sorted(g["boxes"])
                g["reasons_cn"] = {k: ACCEPT_REASONS.get(k, k)
                                   for k in g["reasons"]}
                rows.append(g)
            rows.sort(key=lambda x: x["key"])
            return {"group_by": group_by,
                    "clock": s["clock"], "groups": rows}

    def _box_view(self, s: dict, b: dict) -> dict:
        reserved = sum(t["reserved_bytes"] for t in s["tasks"].values()
                       if t["box_id"] == b["box_id"] and t["status"] == "transferring")
        staged = sum(self._staged_bytes(s, t) for t in s["tasks"].values()
                     if t["box_id"] == b["box_id"] and t["status"] == "done"
                     and t["accept_state"] not in ACCEPT_CLOSED)
        quar_bytes = round(sum(q["size_bytes"] for q in b["quarantine"].values()), 1)
        v = copy.deepcopy(b)
        v["quarantine_objects"] = len(b["quarantine"])
        v["quarantine_bytes"] = quar_bytes
        v["proven_bytes"] = round(b.get("proven_bytes", 0.0), 1)
        v["staged_pending_bytes"] = round(staged, 1)
        v["switchable"] = copy.deepcopy(s["switchable"].get(b["box_id"], []))
        v["switchable_count"] = len(s["switchable"].get(b["box_id"], []))
        v["reserved_bytes"] = round(reserved, 1)
        v["free_bytes"] = round(b["disk_total_bytes"] - b["disk_used_bytes"]
                                - reserved, 1)
        v["disk_total_mb"] = _round6(b["disk_total_bytes"] / 1e6)
        v["disk_used_mb"] = _round6(b["disk_used_bytes"] / 1e6)
        v["reserved_mb"] = _round6(reserved / 1e6)
        v["inventory_objects"] = len(b["inventory"])
        return v

    # ======================================================== invariants ==
    def check_invariants(self) -> list:
        """Return a list of invariant violations (empty means healthy)."""
        with self.lock:
            s = self.state
            bad = []
            plan = self._plan(s)
            # 1. site bandwidth never exceeded
            for site in s["sites"].values():
                used = sum(p["grant_kbps"] for tid, p in plan.items()
                           if s["tasks"][tid]["site_id"] == site["site_id"])
                if used > site["capacity_kbps"] + EPS:
                    bad.append(f"站点 {site['site_id']} 超带宽: "
                               f"{used} > {site['capacity_kbps']}")
            # 2. paired acquire/release: transferring ⇔ reservation held
            for t in s["tasks"].values():
                if t["status"] == "transferring":
                    if t["reserved_bytes"] <= EPS < self._remaining(t):
                        bad.append(f"任务 {t['task_id']} 传输中但无磁盘预留")
                elif t["reserved_bytes"] > EPS:
                    bad.append(f"任务 {t['task_id']} 非传输中却持有预留（幽灵占用）")
            # 3. box disk never exceeded (used + reservations ≤ total)
            for b in s["boxes"].values():
                reserved = sum(t["reserved_bytes"] for t in s["tasks"].values()
                               if t["box_id"] == b["box_id"]
                               and t["status"] == "transferring")
                if b["disk_used_bytes"] + reserved > b["disk_total_bytes"] + EPS:
                    bad.append(f"盒子 {b['box_id']} 磁盘超占: "
                               f"{b['disk_used_bytes']}+{reserved} > "
                               f"{b['disk_total_bytes']}")
            # 4. only valid proofs enter the switchable set
            for bid, entries in s["switchable"].items():
                for e in entries:
                    a = s["acceptances"].get(e["proof_id"])
                    if a is None or not a["valid"]:
                        bad.append(f"盒子 {bid} 可切换集合含无效证明 {e['proof_id']}")
            # 5. acceptance state consistency / no stranded repair capacity
            for rid, r in s["repairs"].items():
                rt = s["tasks"].get(r["task_id"])
                if rt is None:
                    bad.append(f"修复 {rid} 无任务记录")
                    continue
                if r["status"] in ("queued", "transferring", "paused"):
                    parent = s["tasks"].get(r["parent_task_id"])
                    if parent and parent["accept_state"] != ACCEPT_REPAIRING:
                        bad.append(f"修复 {rid} 进行中但父任务状态为 "
                                   f"{parent['accept_state']}")
                if rt["status"] not in TASK_OPEN + ("paused",) \
                        and rt["reserved_bytes"] > EPS:
                    bad.append(f"修复 {rid} 已终结却残留预留（容量占用泄漏）")
            for tid, t in s["tasks"].items():
                if t.get("kind") == "repair":
                    continue
                if t["status"] == "done" and t["accept_state"] == ACCEPT_PROVEN \
                        and not t.get("proof_id"):
                    bad.append(f"任务 {tid} 标记已证明但无证明编号")
            # 6. immutable acceptance ledger: digests must recompute
            for aid, a in s["acceptances"].items():
                payload = {k: a[k] for k in (
                    "manifest_id", "manifest_hash", "generations", "version",
                    "job_id", "task_id", "repair_for", "box_id", "site_id",
                    "box_acceptance_seq", "basis", "root_hash", "root_observed",
                    "root_ok", "objects_total", "objects_proven",
                    "objects_quarantined", "chunk_results", "object_results",
                    "reasons", "parent_digest")}
                if sha256(payload) != a["digest"]:
                    bad.append(f"验收 {aid} 证明摘要不可复算（记录被改写）")
            return bad
