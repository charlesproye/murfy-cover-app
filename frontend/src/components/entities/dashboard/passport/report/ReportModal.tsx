import React, { useState } from 'react';
import Modal from '@/components/entities/dashboard/passport/report/Modal';
import ReportContent from '@/components/entities/dashboard/passport/report/ReportContent';
import { IconArrowRight, IconArrowLeft } from '@tabler/icons-react';

interface ReportModalProps {
  onClose: () => void;
  data: unknown;
}

const ReportModal: React.FC<ReportModalProps> = ({ onClose, data }) => {
  const [currentPage, setCurrentPage] = useState<1 | 2>(1);

  const togglePage = (): void => {
    setCurrentPage(currentPage === 1 ? 2 : 1);
  };

  // Contrôle gauche (flèche précédente)
  const leftControl =
    currentPage === 2 ? (
      <button
        onClick={togglePage}
        className="flex items-center justify-center bg-white rounded-full w-10 h-10 shadow-md hover:bg-gray-100"
      >
        <IconArrowLeft className="w-5 h-5" />
      </button>
    ) : null;

  // Contrôle droit (flèche suivante)
  const rightControl =
    currentPage === 1 ? (
      <button
        onClick={togglePage}
        className="flex items-center justify-center bg-white rounded-full w-10 h-10 shadow-md hover:bg-gray-100"
      >
        <IconArrowRight className="w-5 h-5" />
      </button>
    ) : null;

  return (
    <Modal onClose={onClose} leftControl={leftControl} rightControl={rightControl}>
      <ReportContent data={data} currentPage={currentPage} />
    </Modal>
  );
};

export default ReportModal;
