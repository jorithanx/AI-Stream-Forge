const KEY = "cp_token";

export const getToken = (): string => localStorage.getItem(KEY) ?? "";
export const setToken = (t: string): void => { localStorage.setItem(KEY, t); };
export const clearToken = (): void => { localStorage.removeItem(KEY); };

// hobby-session-34

// hobby-session-88

// hobby-session-9
