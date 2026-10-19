const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const routeKeys = new Set([
  "baseurl",
  "openaibaseurl",
  "envkey",
  "apikey",
  "experimentalbearertoken",
  "requiresopenaiauth",
  "wireapi",
  "authmode",
  "configfile",
]);

/** Merged config/read includes system and managed layers absent from local file scans. */
export function codexSubscriptionConfigError(response: unknown): string | undefined {
  if (!record(response) || !record(response.config) || !Array.isArray(response.layers))
    return "Codex did not return its merged configuration and layers.";
  const config = response.config;
  for (const key of [
    "model_provider",
    "model_providers",
    "forced_login_method",
    "profile",
    "profiles",
    "openai_base_url",
    "chatgpt_base_url",
  ])
    if (!Object.hasOwn(config, key) || config[key] === undefined)
      return `Codex cannot verify its effective ${key} setting.`;

  const inspect = (value: unknown, path: string): string | undefined => {
    if (Array.isArray(value)) {
      for (const [index, nested] of value.entries()) {
        const error = inspect(nested, `${path}[${index}]`);
        if (error !== undefined) return error;
      }
      return;
    }
    if (!record(value)) return;
    for (const [key, nested] of Object.entries(value)) {
      const normalized = key.toLowerCase().replaceAll("_", "").replaceAll("-", "");
      const location = `${path}.${key}`;
      if (nested !== null && nested !== undefined) {
        if (
          routeKeys.has(normalized) ||
          (normalized === "chatgptbaseurl" && nested !== "https://chatgpt.com/backend-api/") ||
          (normalized === "modelprovider" && nested !== "openai") ||
          (normalized === "forcedloginmethod" && nested !== "chatgpt") ||
          normalized === "profile" ||
          ((normalized === "modelproviders" || normalized === "profiles") &&
            (!record(nested) || Object.keys(nested).length !== 0))
        )
          return `Codex subscription-only delegation refuses ${location}.`;
      }
      const error = inspect(nested, location);
      if (error !== undefined) return error;
    }
  };
  return inspect(config, "config");
}

export function codexSubscriptionNativeRouteError(response: unknown): string | undefined {
  if (
    !record(response) ||
    response.modelProvider !== "openai" ||
    !record(response.thread) ||
    response.thread.modelProvider !== "openai"
  )
    return "Codex did not confirm the built-in OpenAI route for this native thread.";
}
