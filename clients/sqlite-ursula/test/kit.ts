import { inject } from "vitest";

/** The base URL of the server the suite runs against (test/stack.ts). */
export const ursulaUrl = (): string => inject("ursulaUrl");
