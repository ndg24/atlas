import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { LoginForm } from "@/components/login-form";

const pushMock = vi.fn();
const refreshMock = vi.fn();
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: pushMock, refresh: refreshMock }),
}));

describe("LoginForm", () => {
  beforeEach(() => {
    pushMock.mockClear();
    refreshMock.mockClear();
    vi.stubGlobal("fetch", vi.fn());
  });

  it("defaults to login mode and posts email/password with no workspace_name", async () => {
    (fetch as unknown as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ ok: true }),
    });

    render(<LoginForm />);
    fireEvent.change(screen.getByPlaceholderText("email"), { target: { value: "a@b.com" } });
    fireEvent.change(screen.getByPlaceholderText("password"), { target: { value: "secret123" } });
    fireEvent.click(screen.getByRole("button", { name: /^log in$/i }));

    await waitFor(() => expect(pushMock).toHaveBeenCalledWith("/datasets"));
    expect(refreshMock).toHaveBeenCalled();

    expect(fetch).toHaveBeenCalledWith(
      "/api/auth",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ mode: "login", email: "a@b.com", password: "secret123" }),
      }),
    );
  });

  it("toggles to signup mode and reveals the workspace name field", () => {
    render(<LoginForm />);
    expect(screen.queryByPlaceholderText(/workspace name/i)).not.toBeInTheDocument();

    fireEvent.click(screen.getByText(/need an account\?/i));

    expect(screen.getByPlaceholderText(/workspace name/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /create account/i })).toBeInTheDocument();
  });

  it("shows the server's error message on a failed request without navigating", async () => {
    (fetch as unknown as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      json: async () => ({ error: "invalid email or password" }),
    });

    render(<LoginForm />);
    fireEvent.change(screen.getByPlaceholderText("email"), { target: { value: "a@b.com" } });
    fireEvent.change(screen.getByPlaceholderText("password"), { target: { value: "wrong" } });
    fireEvent.click(screen.getByRole("button", { name: /^log in$/i }));

    expect(await screen.findByText("invalid email or password")).toBeInTheDocument();
    expect(pushMock).not.toHaveBeenCalled();
  });
});
