type Bar = { x: number; w: number; h: number; o: number };

// One hand-composed 240px measure: pair, single, triplet, single, pair,
// single. Gaps come in three tiers (7 / 27-38 / 47) — grouped like a rhythm,
// never a void, never a picket fence. Uniform lines; spacing is the voice.
const TILE: Bar[] = [
  { x: 4, w: 2, h: 16, o: 1 },
  { x: 11, w: 2, h: 16, o: 1 },
  { x: 38, w: 2, h: 16, o: 1 },
  { x: 66, w: 2, h: 16, o: 1 },
  { x: 73, w: 2, h: 16, o: 1 },
  { x: 80, w: 2, h: 16, o: 1 },
  { x: 118, w: 2, h: 16, o: 1 },
  { x: 152, w: 2, h: 16, o: 1 },
  { x: 159, w: 2, h: 16, o: 1 },
  { x: 206, w: 2, h: 16, o: 1 },
];

type Gap = { left: number; width: number; h: number; o: number };

// The gap between two consecutive events: hovering it connects them —
// the earlier bar extends right, meets the later one, then releases.
const GAPS: Gap[] = TILE.slice(0, -1).map((bar, i) => {
  const next = TILE[i + 1] ?? bar;
  return {
    left: bar.x + bar.w,
    width: Math.max(0, next.x - (bar.x + bar.w)),
    h: Math.min(bar.h, next.h),
    o: Math.min(bar.o, next.o),
  };
});

const TILE_WIDTH = 240;

// The seam gap: from each tile's last event across the tile boundary to the
// next tile's first event — otherwise one gap per tile ignores the mouse.
const FIRST = TILE[0];
const LAST = TILE[TILE.length - 1];
const SEAM: Gap | null =
  FIRST && LAST
    ? {
        left: LAST.x + LAST.w,
        width: TILE_WIDTH - (LAST.x + LAST.w) + FIRST.x,
        h: Math.min(LAST.h, FIRST.h),
        o: Math.min(LAST.o, FIRST.o),
      }
    : null;

const TILE_COUNT = 3;

/**
 * The band-edge event stream. Idle: static events in the previous room's
 * material. On band hover, the log replays itself — every event re-appends
 * in offset order with a brief accent flash.
 */
function EdgeStream() {
  // Start on enter, finish on your own time: the class stays until the
  // animation completes, so leaving mid-play never cuts a span short, and
  // every gap plays independently — no blocking between neighbours.
  const startSpan = (event: React.MouseEvent<HTMLElement>) => {
    event.currentTarget.classList.add("on");
  };
  const endSpan = (event: React.AnimationEvent<HTMLElement>) => {
    event.currentTarget.classList.remove("on");
  };

  return (
    <div aria-hidden="true" className="estream">
      {Array.from({ length: TILE_COUNT }, (_, tile) => (
        <div className="estream-tile" key={tile}>
          {TILE.map((bar, i) => (
            <i
              key={i}
              style={{
                left: bar.x,
                width: bar.w,
                height: bar.h,
                opacity: bar.o,
              }}
            />
          ))}
          {(SEAM ? [...GAPS, SEAM] : GAPS).map((gap, i) => (
            <s
              key={`g${i}`}
              onAnimationEnd={endSpan}
              onMouseEnter={startSpan}
              style={
                {
                  left: gap.left,
                  width: gap.width,
                  ["--gh"]: `${gap.h}px`,
                  ["--go"]: String(gap.o),
                } as React.CSSProperties
              }
            />
          ))}
        </div>
      ))}
    </div>
  );
}

export default EdgeStream;
