import { expect, test } from "@playwright/test";
test("operator login, exact approval and observed fake draft", async ({
  page,
  request,
  browser,
}) => {
  const root = process.env.TEST_APPROVAL_ROOT;
  test.skip(!root, "Stage 08 native authenticated services required");
  await page.goto("/#workflow/" + root);
  await expect(
    page.getByRole("heading", { name: "Sign in to ForgeSwarm" }),
  ).toBeVisible();
  await page.getByLabel("Access token").fill(process.env.TEST_OPERATOR_TOKEN!);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Publication approval", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("TEST/DEMO GitHub draft · PENDING", { exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(() => (window as unknown as { pwned?: boolean }).pwned),
  ).toBeUndefined();
  // Read-only credentials can inspect the same immutable evidence, never decide.
  const reader = await browser.newContext();
  const readerPage = await reader.newPage();
  await readerPage.goto("/#workflow/" + root);
  await readerPage
    .getByLabel("Access token")
    .fill(process.env.TEST_READER_TOKEN!);
  await readerPage
    .getByRole("button", { name: "Sign in", exact: true })
    .click();
  await expect(
    readerPage.getByText("Read-only access. An operator must decide."),
  ).toBeVisible();
  await expect(
    readerPage.getByRole("button", { name: "Approve exact candidate" }),
  ).toHaveCount(0);
  await expect(
    readerPage.getByRole("button", { name: "Cancel workflow" }),
  ).toBeDisabled();
  await reader.close();
  await page.getByRole("button", { name: "Approve exact candidate" }).click();
  await expect(
    page.getByText("Publication: confirmed", { exact: true }),
  ).toBeVisible({ timeout: 30000 });
  await expect(
    page.getByText(
      "Controlled fake draft PR #1; no real GitHub PR was created.",
    ),
  ).toBeVisible();
  await expect(page.locator(".status-published")).toBeVisible();
  await expect(
    page.getByText("project.state", { exact: true }).first(),
  ).toBeVisible();
  if (process.env.TEST_SCREENSHOT)
    await page.screenshot({
      path: process.env.TEST_SCREENSHOT,
      fullPage: true,
    });
  const response = await request.post(
    "/v1/approvals/00000000-0000-0000-0000-000000000001",
    {
      data: { decision: "approve" },
      headers: { Authorization: "Bearer worker-or-model-token" },
    },
  );
  expect(response.status()).toBe(401);
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Sign in to ForgeSwarm" }),
  ).toBeVisible();
});
