import { cell, types } from "../../../jsr-cells/mod.ts";

let alarmCount = 0;

cell.request(async (req: Request) => {
  const url = new URL(req.url);
  if (url.pathname.split("/").at(-1) === "getAlarmCount") {
    return Response.json({ count: alarmCount });
  }

  switch (req.method) {
    case "GET": {
      const id = url.searchParams.get("id");
      const alarm = cell.getAlarm(id ? types.scheduledTaskId(id) : undefined);
      return Response.json(alarm);
    }
    case "POST": {
      const alarmSchedule = await req.json();
      if (typeof alarmSchedule !== "number") {
        return Response.json({ error: "Invalid alarm schedule" }, {
          status: 400,
        });
      }
      const id = await cell.setAlarm(Date.now() + alarmSchedule);
      return new Response(id);
    }
    case "DELETE": {
      const id = url.searchParams.get("id");
      if (id === null) {
        return Response.json({ error: "id is required" }, {
          status: 400,
        });
      }
      const deleted = cell.deleteAlarm(id as types.ScheduledTaskId);
      return Response.json({ deleted });
    }
    default: {
      return Response.json({ error: "Method not allowed" }, {
        status: 405,
      });
    }
  }
});

cell.alarm(() => {
  alarmCount++;
});
