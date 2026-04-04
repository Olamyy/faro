import {Routes, Route, Outlet} from "react-router-dom";
import Explore from "./pages/Explore";

export default function App() {
  return (
    <Routes>
      <Route element={
          (
              <div className="h-screen bg-zinc-50 dark:bg-zinc-950 text-zinc-900 dark:text-zinc-100 overflow-hidden flex flex-col">
                  <Outlet />
              </div>
          )
      }>
        <Route index element={<Explore />} />
      </Route>
    </Routes>
  );
}
