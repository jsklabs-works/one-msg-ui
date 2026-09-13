// Three-way theme preference (light / dark / system), persisted to
// localStorage and applied as a data-theme attribute on <html> — the
// same attribute index.css's inline bootstrap script sets synchronously
// before first paint (see index.html) so there's no flash on load.
// "system" means no attribute at all: the CSS's prefers-color-scheme
// media query decides, and it keeps following the OS live if it changes
// while the page is open.

import { useEffect, useState } from "react";

export type ThemePreference = "light" | "dark" | "system";

const STORAGE_KEY = "one-msg-ui-theme";

function applyTheme(pref: ThemePreference) {
  const root = document.documentElement;
  if (pref === "system") {
    root.removeAttribute("data-theme");
  } else {
    root.setAttribute("data-theme", pref);
  }
}

function readStoredTheme(): ThemePreference {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark" || stored === "system") return stored;
  } catch {
    // localStorage unavailable (private mode, etc.) — fall back to system
  }
  return "system";
}

export function useTheme(): [ThemePreference, (pref: ThemePreference) => void] {
  const [theme, setThemeState] = useState<ThemePreference>(readStoredTheme);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  function setTheme(pref: ThemePreference) {
    setThemeState(pref);
    try {
      localStorage.setItem(STORAGE_KEY, pref);
    } catch {
      // ignore — the choice just won't survive a reload
    }
  }

  return [theme, setTheme];
}
