/**
 * Reads an integer environment variable. Returns the fallback when the
 * variable is not set, is not a number or is less than `min`.
 */
export function parseEnvInt(
  name: string,
  fallback: number,
  { min = 0 }: { min?: number } = {},
): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value >= min ? value : fallback;
}
