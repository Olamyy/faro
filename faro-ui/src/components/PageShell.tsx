import { Outlet } from "react-router-dom";

export function PageShell() {
  return (
    <div className="h-screen bg-zinc-50 dark:bg-zinc-950 text-zinc-900 dark:text-zinc-100 flex flex-col">
      <Outlet />
    </div>
  );
}
