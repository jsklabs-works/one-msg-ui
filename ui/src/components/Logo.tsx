// The one-msg-ui mark: a hub-and-spoke node diagram — three brokers
// converging on one unified view, which is the entire point of this app.
// Kept as one shared glyph (not three separate emoji/icons) so the header
// mark, the favicon (public/favicon.svg), and the empty-state icon are
// visibly the same logo rather than three different placeholder marks.

// The glyph alone, in white — for dropping into a container that already
// paints its own background (see .brand-mark's gradient tile in index.css).
export function LogoGlyph({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <g stroke="#fff" strokeWidth={1.8} strokeLinecap="round" opacity={0.92}>
        <line x1="12" y1="13" x2="12" y2="5.6" />
        <line x1="12" y1="13" x2="18.2" y2="16.5" />
        <line x1="12" y1="13" x2="5.8" y2="16.5" />
      </g>
      <circle cx="12" cy="5.6" r="2.3" fill="#fff" />
      <circle cx="18.2" cy="16.5" r="2.3" fill="#fff" />
      <circle cx="5.8" cy="16.5" r="2.3" fill="#fff" />
      <circle cx="12" cy="13" r="3" fill="#fff" />
    </svg>
  );
}

// Self-contained version with its own gradient tile background (the same
// gradient as .brand-mark and public/favicon.svg) — for spots with no
// wrapping container to paint one, like the empty-state landing page.
export function LogoMark({ size = 56 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true">
      <defs>
        <linearGradient id="one-msg-ui-logo-tile" x1="0" y1="0" x2="32" y2="32" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#2a78d6" />
          <stop offset="0.55" stopColor="#eb6834" />
          <stop offset="1" stopColor="#1baf7a" />
        </linearGradient>
      </defs>
      <rect width="32" height="32" rx="9" fill="url(#one-msg-ui-logo-tile)" />
      <g stroke="#fff" strokeWidth={2.4} strokeLinecap="round" opacity={0.92}>
        <line x1="16" y1="17" x2="16" y2="7.5" />
        <line x1="16" y1="17" x2="24.23" y2="21.75" />
        <line x1="16" y1="17" x2="7.77" y2="21.75" />
      </g>
      <circle cx="16" cy="7.5" r={3} fill="#fff" />
      <circle cx="24.23" cy="21.75" r={3} fill="#fff" />
      <circle cx="7.77" cy="21.75" r={3} fill="#fff" />
      <circle cx="16" cy="17" r={4} fill="#fff" />
    </svg>
  );
}
