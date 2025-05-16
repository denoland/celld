import { cell } from "../../../jsr-cells/mod.ts";

cell.request(async (req: Request) => {
  switch (req.method) {
    case "GET": {
      const alarm = await cell.getAlarm();
      return Response.json(alarm);
    }
    case "POST": {
      const body = await req.json();
      // Set an alarm in 1 minute
      await cell.setAlarm(Date.now() + 60 * 1000);
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
