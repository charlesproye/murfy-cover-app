'use client';

import React from 'react';

import { getScoreColor, getScoreText } from '@/utils/scoreCardData';
import { ScoreCardProps } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';
import { Loading } from '@/components/common/loading/loading';

const ScoreCard: React.FC<ScoreCardProps> = ({ statTab, title, getIcon, score }) => {
  if (!statTab || statTab.length === 0) {
    return <Loading />;
  }

  const currentScore = statTab[statTab.length - 1];

  return (
    <div className="flex flex-col justify-between bg-white p-3 rounded-[10px] w-[240px] mobile:h-[128px] h-auto">
      <div className="flex items-center space-x-1">
        {getIcon}
        <h2 className="font-medium text-gray text-base">{title}</h2>
      </div>

      <div className="flex flex-col gap-2">
        <div className="flex items-center justify-between">
          <span className="font-bold text-2xl">{currentScore}</span>
          <div
            className={`px-2 py-0.5 rounded-full text-white text-xs ${getScoreColor(score)}`}
          >
            {getScoreText(score)[1]}
          </div>
        </div>
        <div className="text-sm text-black">{getScoreText(score)[0]}</div>
        <div className="w-full rounded-full bg-gray-200 h-2.5 relative">
          {[
            { grade: 'A', color: 'bg-green-500', left: '0' },
            { grade: 'B', color: 'bg-green-400', left: '16.66' },
            { grade: 'C', color: 'bg-yellow-500', left: '33.32' },
            { grade: 'D', color: 'bg-orange-500', left: '49.98' },
            { grade: 'E', color: 'bg-orange-600', left: '66.64' },
            { grade: 'F', color: 'bg-red-500', left: '83.3' },
          ].map(({ grade, color, left }) => (
            <div
              key={grade}
              className={`absolute bottom-0 h-2.5 ${color} w-[16.66%] transition-all duration-300 ${
                score === grade ? 'h-4 shadow-md z-10' : ''
              }`}
              style={{ left: `${left}%` }}
            />
          ))}
        </div>
      </div>
    </div>
  );
};

export default ScoreCard;
