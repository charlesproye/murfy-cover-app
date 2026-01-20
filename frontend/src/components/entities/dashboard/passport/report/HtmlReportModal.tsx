import React from 'react';
import Modal from '@/components/entities/dashboard/passport/report/Modal';
import useGetReportHtml from '@/hooks/dashboard/passport/useGetReportHtml';

interface HtmlReportModalProps {
  onClose: () => void;
  vin: string;
  reportType?: 'premium' | 'readout';
}

const HtmlReportModal: React.FC<HtmlReportModalProps> = ({
  onClose,
  vin,
  reportType = 'premium'
}) => {
  const { htmlContent, isLoading, error } = useGetReportHtml(vin, reportType);

  const scaledHtmlContent = React.useMemo(() => {
    if (!htmlContent) return null;
    // Inject script to scale the A4 report (595px width) to fit the iframe width
    const script = `
      <script>
        function fitToWidth() {
          const REPORT_WIDTH = 595;
          const scale = window.innerWidth / REPORT_WIDTH;
          document.body.style.transform = 'scale(' + scale + ')';
          document.body.style.transformOrigin = 'top left';
          document.body.style.width = REPORT_WIDTH + 'px';
          document.body.style.overflowX = 'hidden';
        }
        window.addEventListener('resize', fitToWidth);
        document.addEventListener('DOMContentLoaded', fitToWidth);
        fitToWidth();
      </script>
    `;
    return htmlContent + script;
  }, [htmlContent]);

  return (
    <Modal onClose={onClose} maxWidth="max-w-7xl">
      <div className="w-full h-[85vh]">
        {isLoading && (
          <div className="flex items-center justify-center h-full">
            <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin"></div>
          </div>
        )}
        {!!error && !isLoading && !htmlContent && (
          <div className="flex items-center justify-center h-full text-red-500">
            <p>Failed to load report. Please try again.</p>
          </div>
        )}
        {scaledHtmlContent && (
          <iframe
            srcDoc={scaledHtmlContent}
            className="w-full h-full border-0"
            title="Premium Report"
            sandbox="allow-scripts allow-same-origin"
          />
        )}
      </div>
    </Modal>
  );
};

export default HtmlReportModal;
