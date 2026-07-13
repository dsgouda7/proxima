import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import AppRadio from './AppRadio';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AppRadio />
  </StrictMode>
);
