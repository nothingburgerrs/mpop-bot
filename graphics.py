"""Graphics engine: renders finished result images from templates.

The design goal is that adding a new graphic means adding a LAYOUT entry and a
small builder function, not writing new drawing code. Music Core is the first
template; Spotify Counter posts, other music shows, chart graphics and
milestone cards are meant to reuse render_template() as-is.

A layout is a plain dict describing where things go on a template image:

    "music_core": {
        "file":   template filename, relative to this module
        "size":   (width, height) the coordinates below assume
        "repeat": optional {"count": n, "dx": px, "dy": px} to stamp the same
                  slots across several panels
        "slots":  list of TextSlot / ImageSlot
    }

Coordinates are plain pixels. Use render_calibration() to draw every slot as a
labelled box over the template so they can be checked and nudged by eye.
"""

import io
import os

# Pillow is imported lazily so that a missing install degrades to a clear
# message from the command rather than breaking the bot at startup.
try:
    from PIL import Image, ImageDraw, ImageFont
    PILLOW_AVAILABLE = True
except ImportError:  # pragma: no cover
    PILLOW_AVAILABLE = False

ASSET_DIR = os.path.dirname(os.path.abspath(__file__))

# First font that exists wins. Korean labels are already baked into the
# template, so these only need to cover Latin text and digits.
FONT_CANDIDATES = [
    os.path.join(ASSET_DIR, "assets", "font_bold.ttf"),
    r"C:\Windows\Fonts\arialbd.ttf",
    r"C:\Windows\Fonts\segoeuib.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]

WHITE = (255, 255, 255)
DARK = (26, 26, 26)


# --- Slot definitions ---------------------------------------------------

class TextSlot:
    """A piece of text. Shrinks automatically if it would overflow max_width."""

    def __init__(self, name, xy, size=40, color=DARK, anchor="lm", max_width=None,
                 uppercase=False, max_lines=1, min_size=18, stroke=0, stroke_color=None):
        self.name = name
        self.xy = xy
        self.size = size
        self.color = color
        self.anchor = anchor  # PIL anchors: l/m/r + a/m/s/b
        self.max_width = max_width
        self.uppercase = uppercase
        self.max_lines = max_lines      # Wrap onto up to this many lines
        self.min_size = min_size        # Never shrink below this
        self.stroke = stroke            # Outline width, for text over artwork
        self.stroke_color = stroke_color


class ImageSlot:
    """An image placed into a box, cropped to fill it without distortion.

    layer="behind" puts the image underneath the template, so it shows through
    the template's transparent areas and the frame art stays on top. This is
    what era images want. layer="above" pastes over the template instead.
    """

    def __init__(self, name, box, fit="cover", layer="behind"):
        self.name = name
        self.box = box  # (x, y, w, h)
        self.fit = fit
        self.layer = layer


# --- Layouts ------------------------------------------------------------
#
# Music Core: three nominee panels, identical except for a horizontal offset.
# Panel 1 starts at x=128 and each subsequent panel is 580px to the right.

MUSIC_CORE_SLOTS = [
    # Behind the template, so it shows through the transparent window and the
    # teal frame art stays on top of it.
    ImageSlot("era_image", (137, 127, 488, 266), layer="behind"),
    TextSlot("group_name", (383, 424), size=42, color=DARK, anchor="mm", max_width=470,
             uppercase=True, max_lines=2, min_size=24),
    TextSlot("song_title", (383, 482), size=34, color=WHITE, anchor="mm", max_width=470,
             max_lines=2, min_size=20, stroke=2, stroke_color=(0, 0, 0)),
    TextSlot("score_sound", (600, 567), size=34, color=DARK, anchor="rm"),
    TextSlot("score_video", (600, 627), size=34, color=DARK, anchor="rm"),
    TextSlot("score_prevote", (600, 695), size=34, color=DARK, anchor="rm"),
    TextSlot("score_livevote", (600, 770), size=34, color=DARK, anchor="rm"),
    TextSlot("score_total", (600, 870), size=52, color=DARK, anchor="rm"),
]

# Inkigayo: same three-panel shape as Music Core, six score rows instead of four.
# Row order follows the template's own legend, which lists physical first and
# digital last - not weight order.
# Inkigayo, matching the supplied example:
#   - group name sits left-aligned on the magenta strip, as "NAME (한글)"
#   - song title sits right-aligned on the blue strip below it
#   - the six scores are large and centred, in navy
#   - the total is large and centred; the leader's total is highlighted yellow
INKIGAYO_NAVY = (26, 35, 126)

INKIGAYO_SLOTS = [
    ImageSlot("era_image", (519, 97, 412, 282), layer="behind"),
    TextSlot("group_name", (537, 399), size=30, color=WHITE, anchor="lm", max_width=380,
             max_lines=1, min_size=17),
    TextSlot("song_title", (917, 443), size=30, color=WHITE, anchor="rm", max_width=380,
             max_lines=1, min_size=17),
    TextSlot("score_physical", (725, 530), size=52, color=INKIGAYO_NAVY, anchor="mm", min_size=30),
    TextSlot("score_sns", (725, 596), size=52, color=INKIGAYO_NAVY, anchor="mm", min_size=30),
    TextSlot("score_prevote", (725, 663), size=52, color=INKIGAYO_NAVY, anchor="mm", min_size=30),
    TextSlot("score_onair", (725, 730), size=52, color=INKIGAYO_NAVY, anchor="mm", min_size=30),
    TextSlot("score_livevote", (725, 797), size=52, color=INKIGAYO_NAVY, anchor="mm", min_size=30),
    TextSlot("score_digital", (725, 864), size=52, color=INKIGAYO_NAVY, anchor="mm", min_size=30),
    TextSlot("score_total", (725, 955), size=58, color=WHITE, anchor="mm", min_size=32),
]

# M Countdown: head-to-head, two nominees only. Its own layout rather than a
# repeat, because the two sides are not offset copies - the score rows sit in a
# shared centre panel with one nominee's number on each side.
MCOUNTDOWN_SCORE_ROWS = ("digital", "physical", "social", "fanvote", "broadcast")
MCOUNTDOWN_ROW_Y = (699, 745, 792, 838, 885)
MCOUNTDOWN_TOTAL_Y = 931
MCOUNTDOWN_LEFT_X = 671    # left nominee's scores, left-aligned
MCOUNTDOWN_RIGHT_X = 1248  # right nominee's scores, right-aligned

MCOUNTDOWN_SLOTS = [
    ImageSlot("era_image_left", (137, 266, 796, 439), layer="behind"),
    ImageSlot("era_image_right", (1006, 266, 795, 439), layer="behind"),

    TextSlot("group_left", (374, 762), size=32, color=DARK, anchor="mm",
             max_width=440, uppercase=True, min_size=20),
    TextSlot("song_left", (374, 812), size=32, color=(37, 78, 196), anchor="mm",
             max_width=440, uppercase=True, max_lines=1, min_size=18),
    TextSlot("group_right", (1552, 762), size=32, color=DARK, anchor="mm",
             max_width=440, uppercase=True, min_size=20),
    TextSlot("song_right", (1552, 812), size=32, color=(37, 78, 196), anchor="mm",
             max_width=440, uppercase=True, max_lines=1, min_size=18),
] + [
    slot
    for index, row in enumerate(MCOUNTDOWN_SCORE_ROWS)
    for slot in (
        TextSlot(f"left_{row}", (MCOUNTDOWN_LEFT_X, MCOUNTDOWN_ROW_Y[index]),
                 size=26, color=WHITE, anchor="lm"),
        TextSlot(f"right_{row}", (MCOUNTDOWN_RIGHT_X, MCOUNTDOWN_ROW_Y[index]),
                 size=26, color=WHITE, anchor="rm"),
    )
] + [
    TextSlot("left_total", (MCOUNTDOWN_LEFT_X, MCOUNTDOWN_TOTAL_Y),
             size=30, color=(255, 214, 0), anchor="lm"),
    TextSlot("right_total", (MCOUNTDOWN_RIGHT_X, MCOUNTDOWN_TOTAL_Y),
             size=30, color=(255, 214, 0), anchor="rm"),
]

LAYOUTS = {
    "music_core": {
        "file": "music_core_template.png",
        "size": (1920, 1080),
        "repeat": {"count": 3, "dx": 580, "dy": 0},
        "slots": MUSIC_CORE_SLOTS,
    },
    "inkigayo": {
        "file": "inkigayo_template.png",
        "size": (1920, 1080),
        "repeat": {"count": 3, "dx": 437, "dy": 0},
        "slots": INKIGAYO_SLOTS,
    },
    "mcountdown": {
        "file": "mcountdown_template.png",
        "size": (1920, 1080),
        # No repeat: a single panel holding both nominees.
        "slots": MCOUNTDOWN_SLOTS,
    },
}


# --- Rendering ----------------------------------------------------------

def _load_font(size):
    for path in FONT_CANDIDATES:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


def _text_width(draw, text, font):
    return draw.textbbox((0, 0), text, font=font)[2]


def _wrap(draw, text, font, max_width, max_lines):
    """Greedy word wrap. Returns None if it cannot fit in max_lines."""
    words = text.split()
    if not words:
        return [""]
    lines, current = [], words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _text_width(draw, candidate, font) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
            if len(lines) >= max_lines:
                return None
    lines.append(current)
    if len(lines) > max_lines or any(_text_width(draw, l, font) > max_width for l in lines):
        return None
    return lines


def _ellipsize(draw, text, font, max_width):
    """Trim with a trailing ellipsis until it fits. Last resort."""
    if _text_width(draw, text, font) <= max_width:
        return text
    trimmed = text
    while trimmed and _text_width(draw, trimmed + "…", font) > max_width:
        trimmed = trimmed[:-1]
    return (trimmed.rstrip() + "…") if trimmed else "…"


def _layout_text(draw, text, slot):
    """Fit text into the slot: shrink, then wrap, then ellipsize.

    Long album names like 'CHRYSALIS (English Ver + Sped up Ver.)' would
    otherwise run straight out of their panel.
    """
    size = slot.size
    if not slot.max_width:
        return [text], _load_font(size)

    while size >= slot.min_size:
        font = _load_font(size)
        if _text_width(draw, text, font) <= slot.max_width:
            return [text], font
        if slot.max_lines > 1:
            wrapped = _wrap(draw, text, font, slot.max_width, slot.max_lines)
            if wrapped:
                return wrapped, font
        size -= 2

    # Still too big at the minimum size: wrap if allowed, otherwise cut it.
    font = _load_font(slot.min_size)
    if slot.max_lines > 1:
        wrapped = _wrap(draw, text, font, slot.max_width, slot.max_lines)
        if wrapped:
            return wrapped, font
    return [_ellipsize(draw, text, font, slot.max_width)], font


def _fit_image(img, box_w, box_h, mode="cover"):
    """Resize preserving aspect ratio; 'cover' crops the overflow."""
    img = img.convert("RGBA")
    src_w, src_h = img.size
    if src_w == 0 or src_h == 0:
        return img

    scale = max(box_w / src_w, box_h / src_h) if mode == "cover" else min(box_w / src_w, box_h / src_h)
    new_size = (max(1, int(src_w * scale)), max(1, int(src_h * scale)))
    img = img.resize(new_size, Image.LANCZOS)

    if mode == "cover":
        left = (img.width - box_w) // 2
        top = (img.height - box_h) // 2
        img = img.crop((left, top, left + box_w, top + box_h))
    return img


def _iter_slots(layout):
    """Yields (slot, panel_index, dx, dy) for every slot, expanding repeats."""
    repeat = layout.get("repeat")
    if not repeat:
        for slot in layout["slots"]:
            yield slot, 0, 0, 0
        return
    for panel in range(repeat["count"]):
        for slot in layout["slots"]:
            yield slot, panel, repeat["dx"] * panel, repeat["dy"] * panel


WINNER_GOLD = (255, 214, 0)


def render_template(layout_name, panel_data, images=None, color_overrides=None):
    """Render a template.

    panel_data:      list of dicts, one per panel, mapping slot name -> text.
    images:          dict of (panel_index, slot_name) -> PIL Image or raw bytes.
    color_overrides: dict of (panel_index, slot_name) -> RGB tuple, used to
                     highlight the winner's total.

    Returns a BytesIO holding a PNG, ready for discord.File.
    """
    if not PILLOW_AVAILABLE:
        raise RuntimeError("Pillow is not installed. Run: pip install Pillow")

    layout = LAYOUTS[layout_name]
    template_path = os.path.join(ASSET_DIR, layout["file"])
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template image not found: {template_path}")

    template = Image.open(template_path).convert("RGBA")
    images = images or {}
    color_overrides = color_overrides or {}

    def place(target, wanted_layer):
        for slot, panel, dx, dy in _iter_slots(layout):
            if not isinstance(slot, ImageSlot) or slot.layer != wanted_layer:
                continue
            if panel >= len(panel_data):
                continue
            source = images.get((panel, slot.name))
            if source is None:
                continue
            try:
                img = source if hasattr(source, "convert") else Image.open(io.BytesIO(source))
                x, y, w, h = slot.box
                target.paste(_fit_image(img, w, h, slot.fit), (x + dx, y + dy))
            except Exception as e:
                print(f"GRAPHICS: could not place {slot.name} on panel {panel}: {e}")

    # Artwork goes on its own canvas first, then the template is composited over
    # it, so the template's transparent regions reveal the art and its frames
    # and decorations stay on top.
    base = Image.new("RGBA", template.size, (0, 0, 0, 0))
    place(base, "behind")
    base.alpha_composite(template)
    place(base, "above")

    draw = ImageDraw.Draw(base)
    for slot, panel, dx, dy in _iter_slots(layout):
        if not isinstance(slot, TextSlot) or panel >= len(panel_data):
            continue
        value = panel_data[panel].get(slot.name)
        if value is None:
            continue
        text = str(value).upper() if slot.uppercase else str(value)
        lines, font = _layout_text(draw, text, slot)
        colour = color_overrides.get((panel, slot.name), slot.color)

        line_height = int(font.size * 1.15)
        # Keep the block centred on the anchor when it grew to several lines.
        y_start = slot.xy[1] + dy - (line_height * (len(lines) - 1)) // 2

        for index, line in enumerate(lines):
            draw.text(
                (slot.xy[0] + dx, y_start + index * line_height),
                line,
                font=font,
                fill=colour,
                anchor=slot.anchor,
                stroke_width=slot.stroke,
                stroke_fill=slot.stroke_color,
            )

    out = io.BytesIO()
    base.save(out, format="PNG")
    out.seek(0)
    return out


def render_mcountdown(left, right, left_art=None, right_art=None):
    """Render the M Countdown head-to-head board.

    Separate from the panel-based shows because the two nominees are not offset
    copies of one another: they share a single centre score panel, with one
    nominee's numbers down each side.

    left / right: {'group', 'song', 'scores': {digital, physical, social,
    fanvote, broadcast}, 'total'}
    """
    panel = {
        'group_left': left['group'],
        'song_left': left.get('song', ''),
        'group_right': right['group'],
        'song_right': right.get('song', ''),
        'left_total': f"{left['total']:,}",
        'right_total': f"{right['total']:,}",
    }
    for row in MCOUNTDOWN_SCORE_ROWS:
        panel[f"left_{row}"] = f"{left['scores'].get(row, 0):,}"
        panel[f"right_{row}"] = f"{right['scores'].get(row, 0):,}"

    images = {}
    if left_art:
        images[(0, 'era_image_left')] = left_art
    if right_art:
        images[(0, 'era_image_right')] = right_art

    return render_template("mcountdown", [panel], images)


def render_calibration(layout_name):
    """Draw every slot as a labelled box over the bare template.

    Use this to check the coordinates by eye: text slots show as a crosshair
    with their name, image slots as an outlined rectangle.
    """
    if not PILLOW_AVAILABLE:
        raise RuntimeError("Pillow is not installed. Run: pip install Pillow")

    layout = LAYOUTS[layout_name]
    base = Image.open(os.path.join(ASSET_DIR, layout["file"])).convert("RGBA")
    draw = ImageDraw.Draw(base)
    label_font = _load_font(18)

    for slot, panel, dx, dy in _iter_slots(layout):
        if isinstance(slot, ImageSlot):
            x, y, w, h = slot.box
            draw.rectangle([x + dx, y + dy, x + dx + w, y + dy + h], outline=(255, 0, 0), width=3)
            draw.text((x + dx + 6, y + dy + 6), f"{slot.name} {w}x{h}", font=label_font, fill=(255, 0, 0))
        else:
            x, y = slot.xy[0] + dx, slot.xy[1] + dy
            draw.line([x - 14, y, x + 14, y], fill=(255, 0, 255), width=2)
            draw.line([x, y - 14, x, y + 14], fill=(255, 0, 255), width=2)
            draw.text((x + 16, y - 22), f"{slot.name} ({x},{y})", font=label_font, fill=(255, 0, 255))

    out = io.BytesIO()
    base.save(out, format="PNG")
    out.seek(0)
    return out
