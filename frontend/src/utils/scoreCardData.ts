import { Score } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';

export const getScoreColor = (score: Score): string => {
  switch (score) {
    case 'A':
      return 'bg-green-500';
    case 'B':
      return 'bg-green-400';
    case 'C':
      return 'bg-yellow-500';
    case 'D':
      return 'bg-orange-500';
    case 'E':
      return 'bg-red-500';
    default:
      return 'bg-gray-300';
  }
};

export const getScoreText = (score: Score): [string, string] => {
  switch (score) {
    case 'A':
      return ['Among top 2.5% vehicles', 'Excellent']; // >= 1.45
    case 'B':
      return ['Among top 15% vehicles', 'Good']; // >= 1.30
    case 'C':
      return ['Among top 50% vehicles', 'Average']; // >= 1.05
    case 'D':
      return ['Among bottom 50% vehicles', 'Poor']; // >= 0.70
    case 'E':
      return ['Among bottom 15% vehicles', 'Bad']; // >= 0.25
    default:
      return ['', ''];
  }
};
