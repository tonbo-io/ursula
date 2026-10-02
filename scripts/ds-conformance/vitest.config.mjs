// The package CLI cannot run the built suite: Vitest excludes node_modules from
// test discovery. Include the bundled runner explicitly instead.
export default {
  test: {
    include: ["node_modules/@durable-streams/server-conformance-tests/dist/test-runner.js"],
    exclude: [],
  },
};
