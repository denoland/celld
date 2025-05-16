import { cell } from "../../../jsr-cells/mod.ts";

let alarmCount = 0;

cell.request(async (req: Request) => {
  const url = new URL(req.url);
  if (url.pathname.split("/").at(-1) === "getAlarmCount") {
    return Response.json({ count: alarmCount });
  }

  switch (req.method) {
    case "GET": {
      const alarm = await cell.getAlarm();
      return Response.json(alarm);
    }
    case "POST": {
      const alarmSchedule = await req.json();
      if (typeof alarmSchedule !== "number") {
        return Response.json({ error: "Invalid alarm schedule" }, {
          status: 400,
        });
      }
      await cell.setAlarm(Date.now() + alarmSchedule);
      return Response.json({ success: true });
    }
    case "DELETE": {
      const deleted = await cell.deleteAlarm();
      return Response.json({ deleted });
    }
    default: {
      return Response.json({ error: "Method not allowed" }, {
        status: 405,
      });
    }
  }
});

cell.onAlarm(() => {
  alarmCount++;
});
