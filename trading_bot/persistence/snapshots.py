"""
Snapshot Manager
=================

Manages point-in-time snapshots for recovery and analysis.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging
import gzip

from trading_bot.persistence.store import StateStore, FileStore

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    """Point-in-time snapshot of system state."""
    snapshot_id: str
    timestamp: datetime
    snapshot_type: str  # "scheduled", "event", "manual"
    trigger: str  # What triggered the snapshot
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp.isoformat(),
            "snapshot_type": self.snapshot_type,
            "trigger": self.trigger,
            "data": self.data,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Snapshot':
        return cls(
            snapshot_id=data["snapshot_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            snapshot_type=data["snapshot_type"],
            trigger=data["trigger"],
            data=data["data"],
            metadata=data.get("metadata", {}),
        )


class SnapshotManager:
    """
    Manages system snapshots for recovery and analysis.
    
    Features:
    - Scheduled snapshots (e.g., hourly, daily)
    - Event-triggered snapshots (e.g., before risky operations)
    - Manual snapshots
    - Snapshot retention policies
    - Quick recovery from snapshots
    """
    
    def __init__(
        self,
        base_path: str = "./data/snapshots",
        max_snapshots: int = 100,
        retention_days: int = 30,
    ):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        self.max_snapshots = max_snapshots
        self.retention_days = retention_days
        
        # Snapshot index
        self._index: Dict[str, Dict] = {}
        self._load_index()
    
    def _load_index(self) -> None:
        """Load snapshot index from disk."""
        index_path = self.base_path / "index.json"
        
        if index_path.exists():
            try:
                with open(index_path, "r") as f:
                    self._index = json.load(f)
            except Exception as e:
                logger.error(f"Error loading snapshot index: {e}")
                self._index = {}
    
    def _save_index(self) -> None:
        """Save snapshot index to disk."""
        index_path = self.base_path / "index.json"
        
        try:
            with open(index_path, "w") as f:
                json.dump(self._index, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving snapshot index: {e}")
    
    def create_snapshot(
        self,
        data: Dict[str, Any],
        snapshot_type: str = "manual",
        trigger: str = "user",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Snapshot]:
        """
        Create a new snapshot.
        
        Args:
            data: The data to snapshot
            snapshot_type: Type of snapshot (scheduled, event, manual)
            trigger: What triggered the snapshot
            metadata: Additional metadata
        """
        try:
            timestamp = datetime.utcnow()
            snapshot_id = f"{snapshot_type}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            
            snapshot = Snapshot(
                snapshot_id=snapshot_id,
                timestamp=timestamp,
                snapshot_type=snapshot_type,
                trigger=trigger,
                data=data,
                metadata=metadata or {},
            )
            
            # Save snapshot
            if self._save_snapshot(snapshot):
                # Update index
                self._index[snapshot_id] = {
                    "timestamp": timestamp.isoformat(),
                    "type": snapshot_type,
                    "trigger": trigger,
                    "size": len(json.dumps(data)),
                }
                self._save_index()
                
                # Apply retention policy
                self._apply_retention()
                
                logger.info(f"Created snapshot: {snapshot_id}")
                return snapshot
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating snapshot: {e}")
            return None
    
    def _save_snapshot(self, snapshot: Snapshot) -> bool:
        """Save snapshot to disk with compression."""
        try:
            snapshot_path = self.base_path / f"{snapshot.snapshot_id}.json.gz"
            
            with gzip.open(snapshot_path, "wt", encoding="utf-8") as f:
                json.dump(snapshot.to_dict(), f, indent=2, default=str)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving snapshot {snapshot.snapshot_id}: {e}")
            return False
    
    def load_snapshot(self, snapshot_id: str) -> Optional[Snapshot]:
        """Load a snapshot by ID."""
        try:
            snapshot_path = self.base_path / f"{snapshot_id}.json.gz"
            
            if not snapshot_path.exists():
                logger.warning(f"Snapshot not found: {snapshot_id}")
                return None
            
            with gzip.open(snapshot_path, "rt", encoding="utf-8") as f:
                data = json.load(f)
            
            return Snapshot.from_dict(data)
            
        except Exception as e:
            logger.error(f"Error loading snapshot {snapshot_id}: {e}")
            return None
    
    def get_latest_snapshot(
        self,
        snapshot_type: Optional[str] = None,
    ) -> Optional[Snapshot]:
        """Get the most recent snapshot."""
        if not self._index:
            return None
        
        filtered = self._index.items()
        
        if snapshot_type:
            filtered = [(k, v) for k, v in filtered if v.get("type") == snapshot_type]
        
        if not filtered:
            return None
        
        # Sort by timestamp and get latest
        sorted_snapshots = sorted(filtered, key=lambda x: x[1]["timestamp"], reverse=True)
        latest_id = sorted_snapshots[0][0]
        
        return self.load_snapshot(latest_id)
    
    def list_snapshots(
        self,
        snapshot_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List available snapshots."""
        result = []
        
        for snapshot_id, info in self._index.items():
            if snapshot_type and info.get("type") != snapshot_type:
                continue
            
            if since:
                snap_time = datetime.fromisoformat(info["timestamp"])
                if snap_time < since:
                    continue
            
            result.append({
                "snapshot_id": snapshot_id,
                **info,
            })
        
        # Sort by timestamp descending
        result.sort(key=lambda x: x["timestamp"], reverse=True)
        
        return result[:limit]
    
    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot."""
        try:
            snapshot_path = self.base_path / f"{snapshot_id}.json.gz"
            
            if snapshot_path.exists():
                snapshot_path.unlink()
            
            if snapshot_id in self._index:
                del self._index[snapshot_id]
                self._save_index()
            
            logger.info(f"Deleted snapshot: {snapshot_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting snapshot {snapshot_id}: {e}")
            return False
    
    def _apply_retention(self) -> None:
        """Apply retention policy to snapshots."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=self.retention_days)
            to_delete = []
            
            # Find old snapshots
            for snapshot_id, info in self._index.items():
                snap_time = datetime.fromisoformat(info["timestamp"])
                if snap_time < cutoff:
                    to_delete.append(snapshot_id)
            
            # Also check max count
            if len(self._index) > self.max_snapshots:
                sorted_ids = sorted(
                    self._index.keys(),
                    key=lambda x: self._index[x]["timestamp"]
                )
                excess_count = len(self._index) - self.max_snapshots
                to_delete.extend(sorted_ids[:excess_count])
            
            # Delete identified snapshots
            for snapshot_id in set(to_delete):
                self.delete_snapshot(snapshot_id)
            
            if to_delete:
                logger.info(f"Retention policy: deleted {len(to_delete)} snapshots")
                
        except Exception as e:
            logger.error(f"Error applying retention policy: {e}")
    
    def create_recovery_point(
        self,
        bot_state: Dict[str, Any],
        risk_state: Dict[str, Any],
        strategy_states: Dict[str, Dict],
    ) -> Optional[str]:
        """
        Create a complete recovery point.
        
        This is a comprehensive snapshot for disaster recovery.
        """
        data = {
            "bot_state": bot_state,
            "risk_state": risk_state,
            "strategy_states": strategy_states,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        snapshot = self.create_snapshot(
            data=data,
            snapshot_type="recovery",
            trigger="recovery_point",
            metadata={"complete": True},
        )
        
        return snapshot.snapshot_id if snapshot else None
    
    def restore_from_recovery_point(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """Restore system state from a recovery point."""
        snapshot = self.load_snapshot(snapshot_id)
        
        if not snapshot:
            return None
        
        if snapshot.snapshot_type != "recovery":
            logger.warning(f"Snapshot {snapshot_id} is not a recovery point")
        
        return snapshot.data
    
    def get_snapshot_stats(self) -> Dict[str, Any]:
        """Get statistics about snapshots."""
        if not self._index:
            return {"total": 0}
        
        types = {}
        total_size = 0
        
        for info in self._index.values():
            snap_type = info.get("type", "unknown")
            types[snap_type] = types.get(snap_type, 0) + 1
            total_size += info.get("size", 0)
        
        oldest = min(self._index.values(), key=lambda x: x["timestamp"])
        newest = max(self._index.values(), key=lambda x: x["timestamp"])
        
        return {
            "total": len(self._index),
            "by_type": types,
            "total_size_bytes": total_size,
            "oldest": oldest["timestamp"],
            "newest": newest["timestamp"],
        }
