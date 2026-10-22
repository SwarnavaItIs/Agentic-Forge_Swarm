import { expect, test } from "@playwright/test";

test("persisted list → detail → protected evidence → REST cancellation", async ({
  page,
  request,
}) => {
  const root = process.env.TEST_COLLAB_ROOT,
    execution = process.env.TEST_COLLAB_EXECUTION;
  expect(
    root,
    "verification must supply a completed real multi-role fixture",
  ).toBeTruthy();
  expect(execution).toBeTruthy();
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Workflows", exact: true }),
  ).toBeVisible();
  // Follow a real listed execution; UUID selection avoids coupling to display copy.
  await page.getByRole("button", { name: "Execution v1", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(execution!));
  await page
    .getByRole("button")
    .filter({ hasText: "integrate" })
    .first()
    .click();
  await page.getByRole("button", { name: "Test result", exact: true }).click();
  await expect(page.getByLabel("Artifact content")).toContainText(
    "immutable-helper",
  );
  await expect(page.getByLabel("Artifact content")).toContainText(
    '"success":true',
  );
  await page.getByRole("button", { name: "Patch", exact: true }).click();
  await expect(page.getByLabel("Artifact content")).toContainText(
    "+++ b/add.go",
  );
  await page.evaluate(() => window.scrollTo(0, 0));
  if (process.env.TEST_SCREENSHOT)
    await page.screenshot({
      path: process.env.TEST_SCREENSHOT,
      fullPage: true,
    });
  const response = await request.post("/v1/workflows", {
    headers: { "Idempotency-Key": "browser-cancel-" + Date.now() },
    data: {
      name: "Browser cancellation fixture",
      tasks: [{ key: "wait", kind: "delay", delay_ms: 30000 }],
    },
  });
  expect(response.status()).toBe(201);
  const created = await response.json();
  await page.goto("/#workflow/" + created.id);
  await page.getByRole("button", { name: "Cancel workflow" }).click();
  await expect(page.getByRole("status")).toContainText(
    /Cancellation requested|Cancellation settled/,
  );
  await expect
    .poll(async () => {
      const r = await request.get("/v1/workflows/" + created.id);
      return (await r.json()).status;
    })
    .toBe("CANCELLED");
});
