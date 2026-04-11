import { ref } from 'vue';

interface Toast {
  id: number;
  message: string;
  type: 'success' | 'error' | 'info';
}

export function useToast() {
  const toasts = ref<Toast[]>([]);
  let toastId = 0;

  const showToast = (message: string, type: 'success' | 'error' | 'info' = 'success') => {
    const id = toastId++;
    toasts.value.push({ id, message, type });

    setTimeout(() => {
      toasts.value = toasts.value.filter(toast => toast.id !== id);
    }, 3000);
  };

  return {
    toasts,
    showToast
  };
}