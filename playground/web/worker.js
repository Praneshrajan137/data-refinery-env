export default {
  async fetch(request, env) {
    const incoming = new URL(request.url);
    if (incoming.pathname === "/playground") {
      incoming.pathname = "/playground/";
      return Response.redirect(incoming.toString(), 308);
    }

    const assetUrl = new URL(request.url);
    if (assetUrl.pathname.startsWith("/playground/assets/")) {
      assetUrl.pathname = assetUrl.pathname.slice("/playground".length);
    } else if (assetUrl.pathname === "/playground/config.js") {
      assetUrl.pathname = "/config.js";
    } else if (assetUrl.pathname.startsWith("/playground/")) {
      assetUrl.pathname = "/";
    }
    const response = await env.ASSETS.fetch(new Request(assetUrl, request));
    const headers = new Headers(response.headers);
    if (incoming.pathname === "/playground/config.js") {
      headers.set("Cache-Control", "no-store");
    } else if (incoming.pathname.startsWith("/playground/assets/")) {
      headers.set("Cache-Control", "public, max-age=31536000, immutable");
    }
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  },
};
