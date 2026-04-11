<template>
  <div class="w-full bg-gray-200 rounded-full h-4">
    <div 
      class="bg-blue-600 h-4 rounded-full transition-all duration-300"
      :style="{ width: percent + '%' }"
    ></div>
  </div>
</template>

<script setup lang="ts">
defineProps({
  percent: {
    type: Number,
    default: 0
  }
});
</script>