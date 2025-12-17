import React from 'react';
import { Score } from '@/interfaces/common/score';

const scoreLetters: Score[] = ['A', 'B', 'C', 'D', 'E', 'F'];

const getScoreColor = (score: Score): string => {
  switch (score) {
    case 'A':
      return 'bg-green-500';
    case 'B':
      return 'bg-green-400';
    case 'C':
      return 'bg-yellow-400';
    case 'D':
      return 'bg-orange-400';
    case 'E':
      return 'bg-orange-600';
    case 'F':
      return 'bg-red-500';
    default:
      return 'bg-gray-300';
  }
};

const MiniScoreCard: React.FC<{ score: Score }> = ({ score }) => {
  const index = scoreLetters.indexOf(score);
  return (
    <div className="flex items-center w-[90%] justify-center mt-2 min-w-[120px]">
      <div className="relative flex-1 flex items-center h-4">
        <div className="flex w-full h-4 rounded overflow-hidden shadow-xs">
          <div className="flex-1 bg-green-500" />
          <div className="flex-1 bg-green-400" />
          <div className="flex-1 bg-yellow-400" />
          <div className="flex-1 bg-orange-400" />
          <div className="flex-1 bg-orange-600" />
          <div className="flex-1 bg-red-500" />
        </div>
        {index >= 0 && (
          <div
            className={`absolute top-[-8px] h-[32px] w-14 rounded-md shadow-md ${getScoreColor(score)} flex items-center justify-center`}
            style={{ left: `calc(${index} * 16.66% - 3px)` }}
            title={score}
          >
            <span className="text-white text-lg font-bold">{score}</span>
          </div>
        )}
      </div>
    </div>
  );
};

export default MiniScoreCard;
