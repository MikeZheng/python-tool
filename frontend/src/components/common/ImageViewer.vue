<template>
  <Teleport to="body">
    <div
      v-if="visible"
      class="fixed inset-0 z-50 bg-black/90 flex items-center justify-center"
      @click.self="emit('close')"
      @wheel.prevent="onWheel"
    >
      <!-- Toolbar -->
      <div class="absolute top-4 right-4 flex items-center gap-2 z-10">
        <button
          @click="zoomOut"
          class="w-9 h-9 flex items-center justify-center bg-white/20 hover:bg-white/30 text-white rounded text-lg font-bold"
          title="缩小"
        >−</button>
        <span class="text-white text-sm min-w-[48px] text-center">{{ Math.round(scale * 100) }}%</span>
        <button
          @click="zoomIn"
          class="w-9 h-9 flex items-center justify-center bg-white/20 hover:bg-white/30 text-white rounded text-lg font-bold"
          title="放大"
        >+</button>
        <button
          @click="fitToScreen"
          class="px-3 h-9 flex items-center bg-white/20 hover:bg-white/30 text-white rounded text-sm"
          title="适合屏幕"
        >
          适合
        </button>
        <button
          @click="emit('close')"
          class="w-9 h-9 flex items-center justify-center bg-white/20 hover:bg-white/30 text-white rounded text-lg"
          title="关闭"
        >✕</button>
      </div>

      <!-- Prev button -->
      <button
        v-if="images.length > 1"
        @click.stop="navigate(-1)"
        class="absolute left-4 top-1/2 -translate-y-1/2 w-12 h-12 flex items-center justify-center bg-white/20 hover:bg-white/30 text-white rounded-full text-2xl z-10"
      >‹</button>

      <!-- Next button -->
      <button
        v-if="images.length > 1"
        @click.stop="navigate(1)"
        class="absolute right-4 top-1/2 -translate-y-1/2 w-12 h-12 flex items-center justify-center bg-white/20 hover:bg-white/30 text-white rounded-full text-2xl z-10"
      >›</button>

      <!-- Image container -->
      <div
        class="overflow-hidden flex items-center justify-center select-none"
        :class="isDragging ? 'cursor-grabbing' : scale > 1 ? 'cursor-grab' : 'cursor-default'"
        @mousedown="onMouseDown"
        @mousemove="onMouseMove"
        @mouseup="onMouseUp"
        @mouseleave="onMouseUp"
      >
        <div v-if="loading" class="text-white text-lg">加载中...</div>
        <img
          v-show="!loading"
          :src="images[currentIndex]"
          :style="imgStyle"
          class="max-w-none"
          @load="loading = false"
          @dragstart.prevent
        >
      </div>

      <!-- Counter -->
      <div v-if="images.length > 1" class="absolute bottom-6 left-1/2 -translate-x-1/2 text-white text-sm bg-black/50 px-3 py-1 rounded-full">
        {{ currentIndex + 1 }} / {{ images.length }}
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { ref, computed, watch, onUnmounted } from 'vue';

const props = defineProps<{
  images: string[];
  initialIndex: number;
  visible: boolean;
}>();

const emit = defineEmits<{
  (e: 'close'): void;
}>();

const currentIndex = ref(0);
const scale = ref(1);
const offsetX = ref(0);
const offsetY = ref(0);
const loading = ref(true);
// Drag state
const isDragging = ref(false);
const dragStartX = ref(0);
const dragStartY = ref(0);
const imgStyle = computed(() => ({
  transform: `translate(${offsetX.value}px, ${offsetY.value}px) scale(${scale.value})`,
  transition: isDragging.value ? 'none' : 'transform 0.2s ease-out',
}));

const fitToScreen = () => {
  scale.value = 1;
  offsetX.value = 0;
  offsetY.value = 0;
};

const zoomIn = () => {
  scale.value = Math.min(10, scale.value + 0.25);
  clampOffset();
};

const zoomOut = () => {
  scale.value = Math.max(0.1, scale.value - 0.25);
  clampOffset();
};

const onWheel = (e: WheelEvent) => {
  if (e.deltaY < 0) {
    zoomIn();
  } else {
    zoomOut();
  }
};

const clampOffset = () => {
  if (scale.value <= 1) {
    offsetX.value = 0;
    offsetY.value = 0;
  }
};

const navigate = (dir: number) => {
  const next = currentIndex.value + dir;
  if (next >= 0 && next < props.images.length) {
    currentIndex.value = next;
    resetState();
  }
};

const resetState = () => {
  scale.value = 1;
  offsetX.value = 0;
  offsetY.value = 0;
  loading.value = true;
};

// Drag handlers
const onMouseDown = (e: MouseEvent) => {
  if (scale.value <= 1) return;
  isDragging.value = true;
  dragStartX.value = e.clientX - offsetX.value;
  dragStartY.value = e.clientY - offsetY.value;
};

const onMouseMove = (e: MouseEvent) => {
  if (!isDragging.value) return;
  offsetX.value = e.clientX - dragStartX.value;
  offsetY.value = e.clientY - dragStartY.value;
};

const onMouseUp = () => {
  isDragging.value = false;
};

// Keyboard
const onKeydown = (e: KeyboardEvent) => {
  switch (e.key) {
    case 'Escape':
      emit('close');
      break;
    case '+':
    case '=':
      zoomIn();
      break;
    case '-':
      zoomOut();
      break;
    case '0':
      fitToScreen();
      break;
    case 'ArrowLeft':
      navigate(-1);
      break;
    case 'ArrowRight':
      navigate(1);
      break;
  }
};

watch(() => props.visible, async (val) => {
  if (val) {
    currentIndex.value = props.initialIndex;
    resetState();
    document.addEventListener('keydown', onKeydown);
  } else {
    document.removeEventListener('keydown', onKeydown);
  }
});

onUnmounted(() => {
  document.removeEventListener('keydown', onKeydown);
});
</script>
