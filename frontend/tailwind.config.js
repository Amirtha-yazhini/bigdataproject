/** @type {import('tailwindcss').Config} */
const v = (name) => `var(--${name})`;
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        page: v("page"), surface: v("surface"), raised: v("raised"),
        ink: v("ink"), ink2: v("ink-2"), muted: v("muted"), line: v("line"), grid: v("grid"),
        accent: v("accent"), good: v("good"), warning: v("warning"), serious: v("serious"), critical: v("critical"),
      },
      fontFamily: { sans: ["system-ui", "-apple-system", "Segoe UI", "sans-serif"] },
    },
  },
  plugins: [],
};
