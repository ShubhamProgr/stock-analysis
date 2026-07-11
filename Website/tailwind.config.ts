import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: {
          950: "#0b0e14",
          900: "#10141c",
          800: "#171d29",
          700: "#1f2634",
          600: "#262e3d",
          500: "#3a4457"
        },
        text: {
          primary: "#e7ecf5",
          muted: "#8b96ad",
          faint: "#5c6579"
        },
        signal: {
          up: "#3ddc97",
          down: "#ff6b6b",
          amber: "#f2b134"
        }
      },
      fontFamily: {
        display: ["var(--font-display)", "sans-serif"],
        body: ["var(--font-body)", "sans-serif"],
        mono: ["var(--font-mono)", "monospace"]
      },
      boxShadow: {
        panel: "0 1px 0 rgba(255,255,255,0.03) inset, 0 12px 30px -18px rgba(0,0,0,0.6)"
      },
      keyframes: {
        marquee: {
          "0%": { transform: "translateX(0)" },
          "100%": { transform: "translateX(-50%)" }
        }
      },
      animation: {
        marquee: "marquee 38s linear infinite"
      }
    }
  },
  plugins: []
};

export default config;
