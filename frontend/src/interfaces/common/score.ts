export type Score = 'A' | 'B' | 'C' | 'D' | 'E';

export const getScoreColor = (score: Score): string => {
  switch (score) {
    case 'A':
      return 'bg-green-500';
    case 'B':
      return 'bg-lime-400';
    case 'C':
      return 'bg-yellow-300';
    case 'D':
      return 'bg-orange-400';
    case 'E':
      return 'bg-red-500';
    default:
      return 'bg-gray-300';
  }
};
