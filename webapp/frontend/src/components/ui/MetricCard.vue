<script setup lang="ts">
interface Props {
  title: string
  value: string | number
  sublabel?: string
}
defineProps<Props>()
</script>

<template>
  <div class="bg-white/[0.03] p-5 rounded-xl transition-all hover:bg-white/[0.05]">
    <p class="text-[12px] font-semibold text-slate-500 uppercase tracking-wider mb-1">{{ title }}</p>
    <div class="flex items-baseline gap-2">
      <p class="text-3xl font-bold text-white tracking-tight">{{ value }}</p>
      <span v-if="sublabel" class="text-xs text-slate-600 font-medium">{{ sublabel }}</span>
    </div>
  </div>
</template>