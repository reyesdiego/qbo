// Structured logs: one JSON object per line, easy to grep or to ship to any log system.
// Never pass OAuth tokens or credentials in the fields. LOG_LEVEL=silent turns logging off (tests).
type Fields = Record<string, unknown>;
type Level = 'info' | 'warn' | 'error';

const write = (level: Level, message: string, fields: Fields) => {
  if (process.env.LOG_LEVEL === 'silent') return;
  const line = JSON.stringify({ time: new Date().toISOString(), level, message, ...fields });
  if (level === 'error') console.error(line);
  else if (level === 'warn') console.warn(line);
  else console.log(line);
};

export const log = {
  info: (message: string, fields: Fields = {}) => write('info', message, fields),
  warn: (message: string, fields: Fields = {}) => write('warn', message, fields),
  error: (message: string, fields: Fields = {}) => write('error', message, fields),
};
