import "dotenv/config";
import { app } from "./app.js";
import connectDB from "./utils/connectDB.js";

connectDB()
    .then(async () => {
        app.on("error", (error) => {
            console.log("Server issue: ", error);
        });

        app.listen(process.env.PORT, () => {
            console.log(`Server running at: ${process.env.PORT}`);
            console.log(`Health endpoint enabled at: http://localhost:${process.env.PORT}/healthz`);
            console.log(`Metrics endpoint enabled at: http://localhost:${process.env.PORT}/metrics`);
        });
    })
    .catch((error) => {
        console.log("DATABASE connection Failed: ", error);
        process.exit(1);
    });
