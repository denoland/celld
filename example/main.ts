export default {
  fetch(req) {
    const url = new URL(req.url);
    const name = url.searchParams.get("name") || "World";
    
    return new Response(`Hello ${name} from Deno!`, {
      headers: { "Content-Type": "text/plain" },
    });
  }
}
