"use client";

import { useTheme } from "./ThemeProvider";

type ThemeToggleProps = {
  variant?: "segmented" | "button";
  className?: string;
};

export default function ThemeToggle({ variant = "segmented", className = "" }: ThemeToggleProps) {
  const { theme, setTheme, toggleTheme, mounted } = useTheme();

  // Before mount, keep static rendering to avoid hydration mismatch
  const currentTheme = mounted ? theme : "dark";

  if (variant === "button") {
    return (
      <button
        type="button"
        id="theme-toggle-btn"
        className={`themeToggleBtn ${className}`}
        onClick={toggleTheme}
        title={currentTheme === "light" ? "Switch to Dark Mode" : "Switch to Light Mode"}
        aria-label="Toggle light or dark theme"
      >
        {currentTheme === "light" ? (
          <>
            <svg
              width="15"
              height="15"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="themeIcon"
            >
              <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
            </svg>
            <span className="themeBtnLabel">Dark</span>
          </>
        ) : (
          <>
            <svg
              width="15"
              height="15"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="themeIcon"
            >
              <circle cx="12" cy="12" r="5" />
              <line x1="12" y1="1" x2="12" y2="3" />
              <line x1="12" y1="21" x2="12" y2="23" />
              <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
              <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
              <line x1="1" y1="12" x2="3" y2="12" />
              <line x1="21" y1="12" x2="23" y2="12" />
              <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
              <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
            </svg>
            <span className="themeBtnLabel">Light</span>
          </>
        )}
      </button>
    );
  }

  return (
    <div className={`themeSwitchWrap ${className}`} role="group" aria-label="Theme selection">
      <button
        type="button"
        id="theme-toggle-light"
        className={`themeOption ${currentTheme === "light" ? "active" : ""}`}
        onClick={() => setTheme("light")}
        title="Switch to Light Mode"
        aria-pressed={currentTheme === "light"}
      >
        <svg
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="12" cy="12" r="5" />
          <line x1="12" y1="1" x2="12" y2="3" />
          <line x1="12" y1="21" x2="12" y2="23" />
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
          <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
          <line x1="1" y1="12" x2="3" y2="12" />
          <line x1="21" y1="12" x2="23" y2="12" />
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
          <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
        </svg>
        <span>Light</span>
      </button>
      <button
        type="button"
        id="theme-toggle-dark"
        className={`themeOption ${currentTheme === "dark" ? "active" : ""}`}
        onClick={() => setTheme("dark")}
        title="Switch to Dark Mode"
        aria-pressed={currentTheme === "dark"}
      >
        <svg
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
        </svg>
        <span>Dark</span>
      </button>
    </div>
  );
}
