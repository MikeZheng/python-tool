<template>
  <div class="bg-white rounded-lg shadow p-6">
    <div class="text-sm font-medium text-gray-500">{{ title }}</div>
    <div :class="[size || 'text-3xl', 'mt-2 font-bold', color]">{{ value }}</div>
  </div>
</template>

<script setup lang="ts">
defineProps({
  title: {
    type: String,
    required: true
  },
  value: {
    type: [String, Number],
    required: true
  },
  color: {
    type: String,
    default: 'text-gray-900'
  },
  size: {
    type: String,
    default: 'text-3xl'
  }
});
</script>