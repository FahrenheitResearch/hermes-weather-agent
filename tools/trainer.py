"""Background weather model trainer — UNet diffusion on HRRR data."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import numpy as np

# Global state for background training
_training_state = {
    "running": False,
    "epoch": 0,
    "total_epochs": 0,
    "loss": 0.0,
    "best_loss": float("inf"),
    "losses": [],
    "output_dir": "",
    "thread": None,
}


def get_training_status() -> dict:
    return {k: v for k, v in _training_state.items() if k != "thread"}


def start_training(dataset_dir: str, output_dir: str, epochs: int = 50,
                   lr: float = 1e-3, batch_size: int = 4,
                   print_ansi: bool = True) -> dict:
    """Start training a UNet diffusion model in the background."""
    if _training_state["running"]:
        return {"error": "Training already in progress", **get_training_status()}

    dataset = Path(dataset_dir)
    meta_path = dataset / "metadata.json"
    if not meta_path.exists():
        return {"error": f"No metadata.json in {dataset_dir}. Build dataset first."}

    meta = json.loads(meta_path.read_text())
    train_path = dataset / "train.json"
    if not train_path.exists():
        return {"error": "No train.json found"}

    train_pairs = json.loads(train_path.read_text())
    if not train_pairs:
        return {"error": "No training pairs"}

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    _training_state.update({
        "running": True, "epoch": 0, "total_epochs": epochs,
        "loss": 0.0, "best_loss": float("inf"), "losses": [],
        "output_dir": output_dir,
    })

    def train_loop():
        try:
            import torch
            import torch.nn as nn
            import torch.optim as optim

            channels = meta["shape"][0] if meta.get("shape") else 27
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            print(f"\033[1;36m[Train]\033[0m Device: {device}, Channels: {channels}, "
                  f"Pairs: {len(train_pairs)}, Epochs: {epochs}")

            # Simple UNet for weather prediction
            model = SimpleUNet(channels).to(device)
            optimizer = optim.Adam(model.parameters(), lr=lr)
            criterion = nn.MSELoss()

            # Load normalization stats
            norm = meta.get("normalization", {})
            mean = torch.tensor(norm.get("mean", [0]*channels), dtype=torch.float32).view(channels, 1, 1).to(device)
            std = torch.tensor(norm.get("std", [1]*channels), dtype=torch.float32).view(channels, 1, 1).to(device)

            for epoch in range(epochs):
                if not _training_state["running"]:
                    break

                epoch_loss = 0.0
                n_batches = 0

                # Shuffle pairs
                import random
                random.shuffle(train_pairs)

                for i in range(0, len(train_pairs), batch_size):
                    batch = train_pairs[i:i+batch_size]
                    inputs, targets = [], []
                    for pair in batch:
                        try:
                            x = torch.from_numpy(np.load(pair["input"])).float()
                            y = torch.from_numpy(np.load(pair["target"])).float()
                            # Center crop to 256x256 for speed
                            cy, cx = x.shape[1]//2, x.shape[2]//2
                            x = x[:, cy-128:cy+128, cx-128:cx+128]
                            y = y[:, cy-128:cy+128, cx-128:cx+128]
                            # Normalize
                            x = (x - mean.cpu()) / std.cpu()
                            y = (y - mean.cpu()) / std.cpu()
                            inputs.append(x)
                            targets.append(y)
                        except Exception:
                            continue

                    if not inputs:
                        continue

                    x_batch = torch.stack(inputs).to(device)
                    y_batch = torch.stack(targets).to(device)

                    # Add noise for diffusion-style training
                    t = torch.rand(x_batch.shape[0], 1, 1, 1, device=device)
                    noise = torch.randn_like(y_batch)
                    noisy = y_batch * (1 - t) + noise * t

                    # Predict denoised output
                    pred = model(torch.cat([x_batch, noisy], dim=1))
                    loss = criterion(pred, y_batch)

                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

                    epoch_loss += loss.item()
                    n_batches += 1

                avg_loss = epoch_loss / max(n_batches, 1)
                _training_state["epoch"] = epoch + 1
                _training_state["loss"] = avg_loss
                _training_state["losses"].append(avg_loss)
                if avg_loss < _training_state["best_loss"]:
                    _training_state["best_loss"] = avg_loss
                    torch.save(model.state_dict(), str(out / "best_model.pt"))

                # Print progress with mini loss chart
                if print_ansi:
                    bar_width = 40
                    progress = (epoch + 1) / epochs
                    filled = int(bar_width * progress)
                    bar = "\033[42m" + " " * filled + "\033[0m" + "\033[100m" + " " * (bar_width - filled) + "\033[0m"
                    losses = _training_state["losses"]
                    # Mini sparkline
                    if len(losses) > 1:
                        mn, mx = min(losses), max(losses)
                        rng = mx - mn if mx > mn else 1
                        sparks = "".join("▁▂▃▄▅▆▇█"[min(7, int((l-mn)/rng*7))] for l in losses[-20:])
                    else:
                        sparks = ""
                    print(f"\033[1;33m[Epoch {epoch+1}/{epochs}]\033[0m "
                          f"Loss: {avg_loss:.6f} Best: {_training_state['best_loss']:.6f} "
                          f"{bar} {sparks}")

                # Save checkpoint every 10 epochs
                if (epoch + 1) % 10 == 0:
                    torch.save({
                        "epoch": epoch + 1, "model": model.state_dict(),
                        "optimizer": optimizer.state_dict(), "loss": avg_loss,
                    }, str(out / f"checkpoint_e{epoch+1}.pt"))

            # Save final
            torch.save(model.state_dict(), str(out / "final_model.pt"))
            print(f"\033[1;32m[Train Complete]\033[0m Final loss: {avg_loss:.6f} "
                  f"Best: {_training_state['best_loss']:.6f}")
            print(f"  Model saved to {output_dir}/")

        except Exception as e:
            print(f"\033[1;31m[Train Error]\033[0m {e}")
        finally:
            _training_state["running"] = False

    thread = threading.Thread(target=train_loop, daemon=True)
    _training_state["thread"] = thread
    thread.start()

    return {"status": "started", "device": "cuda" if __import__("torch").cuda.is_available() else "cpu",
            "pairs": len(train_pairs), "epochs": epochs, "output_dir": output_dir}


def stop_training() -> dict:
    _training_state["running"] = False
    return {"status": "stopped", **get_training_status()}


class SimpleUNet(object):
    """Lazy import wrapper — actual class defined on first use."""
    _cls = None

    def __new__(cls, channels):
        if cls._cls is None:
            import torch
            import torch.nn as nn
            class _UNet(nn.Module):
                def __init__(self, channels):
                    super().__init__()
                    # Input: [state_t + noisy_target] = 2*channels
                    c = channels
                    self.enc1 = nn.Sequential(nn.Conv2d(c*2, 64, 3, padding=1), nn.ReLU(), nn.Conv2d(64, 64, 3, padding=1), nn.ReLU())
                    self.pool1 = nn.MaxPool2d(2)
                    self.enc2 = nn.Sequential(nn.Conv2d(64, 128, 3, padding=1), nn.ReLU(), nn.Conv2d(128, 128, 3, padding=1), nn.ReLU())
                    self.pool2 = nn.MaxPool2d(2)
                    self.bottleneck = nn.Sequential(nn.Conv2d(128, 256, 3, padding=1), nn.ReLU(), nn.Conv2d(256, 256, 3, padding=1), nn.ReLU())
                    self.up2 = nn.ConvTranspose2d(256, 128, 2, stride=2)
                    self.dec2 = nn.Sequential(nn.Conv2d(256, 128, 3, padding=1), nn.ReLU(), nn.Conv2d(128, 128, 3, padding=1), nn.ReLU())
                    self.up1 = nn.ConvTranspose2d(128, 64, 2, stride=2)
                    self.dec1 = nn.Sequential(nn.Conv2d(128, 64, 3, padding=1), nn.ReLU(), nn.Conv2d(64, 64, 3, padding=1), nn.ReLU())
                    self.final = nn.Conv2d(64, c, 1)

                def forward(self, x):
                    e1 = self.enc1(x)
                    e2 = self.enc2(self.pool1(e1))
                    b = self.bottleneck(self.pool2(e2))
                    d2 = self.dec2(torch.cat([self.up2(b), e2], dim=1))
                    d1 = self.dec1(torch.cat([self.up1(d2), e1], dim=1))
                    return self.final(d1)
            cls._cls = _UNet
        return cls._cls(channels)
