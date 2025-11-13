import React from 'react';

export type ScoreCardProps = {
  statTab?: number[];
  title: string;
  getIcon: React.ReactNode;
  score: Score;
};

export type ScoreCardPropsPdf = {
  statTab?: number[];
  score: Score;
};

export type Score = 'A' | 'B' | 'C' | 'D' | 'E' | 'F';
