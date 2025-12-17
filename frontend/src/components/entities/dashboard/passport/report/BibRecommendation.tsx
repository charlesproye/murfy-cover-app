import React from 'react';
import { Score } from '@/interfaces/common/score';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';

interface BibRecommendationProps {
  score: Score;
  data?: InfoVehicleResult;
}

const BibRecommendation: React.FC<BibRecommendationProps> = ({ score }) => {
  const getRecommendation = (score: Score): { text: string; color: string } => {
    if (score === 'A' || score === 'B') {
      return { text: 'Releasing', color: 'bg-green-500' };
    } else if (score === 'C' || score === 'D') {
      return { text: 'Direct Remarketing', color: 'bg-yellow-500' };
    } else {
      return { text: 'Indirect Remarketing', color: 'bg-red-500' };
    }
  };

  const recommendation = getRecommendation(score);

  return (
    <div className="flex items-center justify-between bg-white rounded-[8px] w-full h-fit relative">
      <div className="flex items-center">
        <h2 className="text-black text-sm">Recommendation</h2>
      </div>
      <div className="flex flex-col gap-1">
        <div
          className={`px-3 py-1 ${recommendation.color} rounded-md text-white text-center font-medium text-xs`}
        >
          {recommendation.text}
        </div>
      </div>
    </div>
  );
};

export default BibRecommendation;
