import React, { useState, useEffect } from 'react';

interface ModalProps {
  onClose: () => void;
  children: React.ReactNode;
  leftControl?: React.ReactNode;
  rightControl?: React.ReactNode;
  maxWidth?: string;
}

const Modal: React.FC<ModalProps> = ({
  onClose,
  children,
  leftControl,
  rightControl,
  maxWidth = 'max-w-4xl',
}) => {
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    setIsOpen(true);
    document.body.style.overflow = 'hidden';

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', handleEscape);

    return () => {
      document.body.style.overflow = 'auto';
      window.removeEventListener('keydown', handleEscape);
    };
  }, [onClose]);

  const handleOverlayClick = (e: React.MouseEvent<HTMLDivElement>): void => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  return (
    <div
      className={`fixed inset-0 z-50 flex h-full items-center justify-center bg-black bg-opacity-50 transition-opacity ${isOpen ? 'opacity-100' : 'opacity-0'}`}
      onClick={handleOverlayClick}
    >
      <div className={`relative ${maxWidth} w-full mx-4`}>
        {/* Contrôle gauche - externe au contenu */}
        {leftControl && (
          <div className="absolute top-1/2 -left-16 -translate-y-1/2">{leftControl}</div>
        )}

        {/* Croix de fermeture */}
        <button
          onClick={onClose}
          className="absolute -top-10 right-0 text-white hover:text-gray-200 z-10 w-8 h-8 flex items-center justify-center rounded-full bg-black/30 hover:bg-black/50"
        >
          ×
        </button>

        {/* Contrôle droit - externe au contenu */}
        {rightControl && (
          <div className="absolute top-1/2 -right-16 -translate-y-1/2">
            {rightControl}
          </div>
        )}

        <div className="bg-white rounded-lg h-full max-h-[90vh] overflow-auto animate-fadeIn">
          {children}
        </div>
      </div>
    </div>
  );
};

export default Modal;
