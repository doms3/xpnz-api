const pad = (n) => n.toString ().padStart (2, '0');

export function getDateString (date = new Date ()) {
  const year = date.getFullYear ();
  const month = pad (date.getMonth () + 1); // Months are zero-based
  const day = pad (date.getDate ());

  return `${year}-${month}-${day}`;
}

export function getDateTimeString (date = new Date ()) {
  const year = date.getFullYear ();
  const month = pad (date.getMonth () + 1); // Months are zero-based
  const day = pad (date.getDate ());
  const hours = pad (date.getHours ());
  const minutes = pad (date.getMinutes ());
  const seconds = pad (date.getSeconds ());

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}
