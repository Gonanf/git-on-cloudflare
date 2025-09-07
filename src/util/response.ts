/**
 * Lightweight response helpers to keep handlers concise and consistent.
 * Prefer these over ad-hoc new Response(...) in endpoint code.
 */

export function json(data: unknown, status = 200, headers: HeadersInit = {}) {
  const h = new Headers(headers);
  if (!h.has("Content-Type")) h.set("Content-Type", "application/json");
  return new Response(JSON.stringify(data), { status, headers: h });
}

export function text(body: string, status = 200, headers: HeadersInit = {}) {
  const h = new Headers(headers);
  if (!h.has("Content-Type")) h.set("Content-Type", "text/plain; charset=utf-8");
  return new Response(body, { status, headers: h });
}

export function badRequest(message = "Bad request\n", headers: HeadersInit = {}) {
  return text(message, 400, headers);
}

export function notFound(message = "Not found\n", headers: HeadersInit = {}) {
  return text(message, 404, headers);
}

export function serverError(message = "Internal error\n", headers: HeadersInit = {}) {
  return text(message, 500, headers);
}
