export type DownloadPropsExtremum = {
  fleet: string;
  quality: 'Best' | 'Worst' | '';
  filename?: string;
  className?: string;
};

export type DownloadProps<T extends Record<string, unknown>> = {
  data: T[];
  filename?: string;
  className?: string;
};
