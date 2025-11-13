export const formatDateToMonthDay = (
  dateString: string | Date,
  formatType: string,
): string => {
  const date = new Date(dateString);

  let options: Intl.DateTimeFormatOptions = {};

  switch (formatType) {
    case 'Week': {
      const startOfWeek = new Date(date);
      startOfWeek.setDate(date.getDate() - date.getDay());
      return `${startOfWeek.toLocaleDateString('en-GB', { month: 'short', day: '2-digit' })}`;
    }
    case 'Month':
      options = { month: 'short', year: 'numeric' };
      break;
    case 'Year':
      options = { year: 'numeric' };
      break;
    default:
      return date.toLocaleDateString('en-GB');
  }

  return date.toLocaleDateString('en-GB', options);
};
