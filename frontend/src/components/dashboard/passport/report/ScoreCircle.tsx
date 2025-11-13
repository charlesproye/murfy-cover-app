import React from 'react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { Score } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';

interface ScoreCircleProps {
  displayScale?: boolean;
  vehicle_battery_info: InfoVehicleResult;
  score: Score;
}

const ScoreCircle: React.FC<ScoreCircleProps> = ({
  displayScale = true,
  vehicle_battery_info,
  score,
}) => {
  const getScoreColor = (letterScore: string): string => {
    switch (letterScore) {
      case 'A':
        return 'bg-green-rapport';
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
        return 'bg-gray';
    }
  };

  const getTextScoreColor = (letterScore: string): string => {
    switch (letterScore) {
      case 'A':
        return 'text-green-rapport';
      case 'B':
        return 'text-green-400';
      case 'C':
        return 'text-yellow-400';
      case 'D':
        return 'text-orange-400';
      case 'E':
        return 'text-orange-600';
      case 'F':
        return 'text-red-500';
      default:
        return 'text-gray';
    }
  };

  return (
    <div className="flex flex-col items-center">
      {/* Bloc SoH design */}
      <div className={`flex  px-4 py-4 w-fit gap-x-2`}>
        <span
          className={`text-4xl font-extrabold ${getTextScoreColor(score)} leading-none`}
        >
          {vehicle_battery_info.battery_info.soh}%
        </span>
        {/* <span className="text-4xl font-semibold text-black opacity-90 tracking-wider">
          SoH
        </span> */}
      </div>
      {/* Échelle de scores en rectangles */}
      {displayScale && (
        <div className="flex flex-col items-center">
          <div className=" mt-4 flex justify-center bg-white items-center w-fit gap-1 rounded-md py-1 px-2">
            {['A', 'B', 'C', 'D', 'E', 'F'].map((letter) => (
              <div
                key={letter}
                className={`
                  ${letter === score ? 'w-16 h-10 text-sm font-extrabold' : 'w-8 h-6 text-xs font-bold'}
                  rounded-md flex items-center justify-center text-white
                  ${getScoreColor(letter)}
                `}
              >
                <p className={`${letter === score ? 'text-xl' : 'text-xs'}`}>{letter}</p>
              </div>
            ))}
          </div>
        </div>
      )}
      <p className="text-xs text-black text-center mt-2 px-1">
        The Bib Score compares the car's State-of-Health (SoH) to similar vehicles -
        similar odometer and age.
      </p>
    </div>
  );
};

export default ScoreCircle;
