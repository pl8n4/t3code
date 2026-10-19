import { assert, describe, it } from "@effect/vitest";
import {
  codexSubscriptionConfigError,
  codexSubscriptionNativeRouteError,
} from "./CodexSubscriptionRoute.ts";

const baseline = {
  model_provider: null,
  forced_login_method: null,
  model_providers: {},
  profiles: {},
  profile: null,
  openai_base_url: null,
  chatgpt_base_url: null,
};
const response = (config: Record<string, unknown>) => ({
  config: { ...baseline, ...config },
  layers: [],
  origins: {},
});

describe("Codex subscription merged route", () => {
  it("accepts built-in defaults and benign configuration", () => {
    assert.isUndefined(
      codexSubscriptionConfigError(
        response({ projects: { "/workspace": { trust_level: "trusted" } }, model: "gpt-6-astra" }),
      ),
    );
    assert.isUndefined(
      codexSubscriptionConfigError(
        response({ model_provider: "openai", forced_login_method: "chatgpt" }),
      ),
    );
    assert.isUndefined(
      codexSubscriptionConfigError(
        response({ chatgpt_base_url: "https://chatgpt.com/backend-api/" }),
      ),
    );
  });
  it.each([
    { model_provider: "custom" },
    { model_providers: { openai: { base_url: "https://example.invalid" } } },
    { openai_base_url: "https://example.invalid" },
    { chatgpt_base_url: "https://example.invalid" },
    { chatgpt_base_url: "http://chatgpt.com/backend-api/" },
    { chatgpt_base_url: "https://chatgpt.com:443/backend-api/" },
    { chatgpt_base_url: "https://user@chatgpt.com/backend-api/" },
    { chatgpt_base_url: "https://chatgpt.com/backend-api/?route=other" },
    { chatgpt_base_url: "https://chatgpt.com/backend-api/#other" },
    { chatgpt_base_url: "https://chatgpt.com/backend-api/custom" },
    { forced_login_method: "api" },
    { profile: "api" },
    { profiles: { api: { model: "anything" } } },
    { agents: { analyst: { config_file: "/tmp/route.toml" } } },
    { agents: { analyst: { api_key: "redacted" } } },
    { future: [{ experimental_bearer_token: "redacted" }] },
  ])("refuses route and role overrides from any merged source: %j", (config) => {
    assert.isString(codexSubscriptionConfigError(response(config)));
  });
  it.each([
    null,
    {},
    { config: baseline },
    response({ model_provider: undefined }),
    { config: {}, layers: [] },
  ])("fails closed for unverifiable runtime responses: %j", (value) => {
    assert.isString(codexSubscriptionConfigError(value));
  });
  it("requires both effective and saved native provider provenance", () => {
    assert.isUndefined(
      codexSubscriptionNativeRouteError({
        modelProvider: "openai",
        thread: { modelProvider: "openai" },
      }),
    );
    for (const value of [
      null,
      {},
      { modelProvider: "openai", thread: { modelProvider: "api" } },
      { modelProvider: "custom", thread: { modelProvider: "openai" } },
    ])
      assert.isString(codexSubscriptionNativeRouteError(value));
  });
});
