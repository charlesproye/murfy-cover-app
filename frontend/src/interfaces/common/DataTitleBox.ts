export type DataTitleBoxExtremum = {
  title: string;
  titleSecondary: string;
  fleet: string | null;
  quality: 'Best' | 'Worst' | '';
  downloadName?: string;
};

export type DataTitleBox = {
  title: string;
  titleSecondary: string;
  dataDownload?: Record<string, unknown>[];
  downloadName?: string;
};
