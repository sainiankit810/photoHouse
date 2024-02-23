import React from "react";
import ReactDOM from "react-dom/client";
import Header from "./components/Header.js"
import Body from "./components/Body"
import View from "./components/View";
import Error from "./components/Error";
import { createBrowserRouter, RouterProvider, Outlet} from "react-router-dom";

const AppLayout = () => {
    return (
        <div className="app bg-black">
            <Header />
            <Outlet />
        </div>
    );
}

const appRouter = createBrowserRouter([
    {
        path: "/",
        element: <AppLayout />,
        children: [
            {
                path: "/",
                element: <Body />,
            },
    
            {
                path: "/getImages",
                element: <View />,
            },
        ],
    
        errorElement: <Error />
    }]
    )

const root = ReactDOM.createRoot(document.getElementById("root"));

root.render(<RouterProvider router={appRouter} />);