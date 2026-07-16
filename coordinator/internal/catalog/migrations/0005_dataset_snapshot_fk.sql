ALTER TABLE datasets
  ADD CONSTRAINT fk_snapshot FOREIGN KEY (current_snapshot_id) REFERENCES snapshots(id);
