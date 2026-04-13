import { ref } from 'vue';

interface Toast {
  id: number;
  message: string;
  type: 'success' | 'error' | 'info';
}

// 单例模式，确保整个应用只有一个toasts数组实例
const toasts = ref<Toast[]>([]);
let toastId = 0;

export function useToast() {
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