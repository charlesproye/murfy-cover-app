import React from 'react';
import { getScoreColor, Score } from '@/interfaces/common/score';

const scoreLetters: Score[] = ['A', 'B', 'C', 'D', 'E'];

const MiniScoreCard: React.FC<{ score: Score }> = ({ score }) => {
  const index = scoreLetters.indexOf(score);
  return (
    <div className="flex items-center w-[90%] justify-center mt-2 min-w-[120px]">
      <div className="relative flex-1 flex items-center h-4">
        <div className="flex w-full h-4 rounded overflow-hidden shadow-xs">
          <div className="flex-1 bg-green-500" />
          <div className="flex-1 bg-lime-400" />
          <div className="flex-1 bg-yellow-300" />
          <div className="flex-1 bg-orange-400" />
          <div className="flex-1 bg-red-500" />
        </div>
        {index >= 0 && (
          <div
            className={`absolute top-[-8px] h-[32px] w-24 md:w-10 lg:w-14 xl:w-17 rounded-md shadow-md ${getScoreColor(score)} flex items-center justify-center`}
            style={{ left: `calc(${index} * 20%)` }}
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
