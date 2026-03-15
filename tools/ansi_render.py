"""High-quality ANSI terminal image renderer using half-block characters."""
import numpy as np


def rgba_to_ansi(pixels: np.ndarray, width: int, height: int, target_width: int = 80) -> str:
    """Render RGBA pixel array to ANSI terminal string using half-block characters.

    Each terminal cell represents 2 vertical pixels using the '▄' character
    with foreground (bottom) and background (top) colors.

    Args:
        pixels: RGBA array, shape (height, width, 4) or flat (height*width*4,)
        width: Image width
        height: Image height
        target_width: Terminal columns to use

    Returns:
        ANSI-escaped string ready for terminal output
    """
    if pixels.ndim == 1:
        pixels = pixels.reshape(height, width, 4)

    # Resize to target width, maintaining aspect ratio
    scale = target_width / width
    new_w = target_width
    new_h = int(height * scale)
    # Make height even for half-block pairs
    new_h = new_h + (new_h % 2)

    # Simple nearest-neighbor resize
    img = np.zeros((new_h, new_w, 4), dtype=np.uint8)
    for y in range(new_h):
        sy = min(int(y / scale), height - 1)
        for x in range(new_w):
            sx = min(int(x / scale), width - 1)
            img[y, x] = pixels[sy, sx]

    lines = []
    for row in range(0, new_h, 2):
        line = []
        for col in range(new_w):
            # Top pixel = background, bottom pixel = foreground
            top = img[row, col]
            bot = img[row + 1, col] if row + 1 < new_h else top

            if top[3] < 10 and bot[3] < 10:
                line.append(" ")
            elif top[3] < 10:
                line.append(f"\033[38;2;{bot[0]};{bot[1]};{bot[2]}m\u2584\033[0m")
            elif bot[3] < 10:
                line.append(f"\033[38;2;{top[0]};{top[1]};{top[2]}m\u2580\033[0m")
            else:
                line.append(
                    f"\033[48;2;{top[0]};{top[1]};{top[2]}m"
                    f"\033[38;2;{bot[0]};{bot[1]};{bot[2]}m\u2584\033[0m"
                )
        lines.append("".join(line))

    return "\n".join(lines)


def render_to_terminal(pixels: np.ndarray, width: int, height: int,
                       target_width: int = 80, title: str = None):
    """Render RGBA pixels directly to terminal with optional title."""
    if title:
        print(f"\033[1m{title}\033[0m")
    ansi = rgba_to_ansi(pixels, width, height, target_width)
    print(ansi)
