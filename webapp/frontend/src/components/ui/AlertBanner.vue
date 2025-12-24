<script setup lang="ts">
import { Info, X } from 'lucide-vue-next'
defineEmits(['close', 'action'])
</script>

<template>
  <div class="bg-indigo-600/10 border border-indigo-500/20 rounded-lg px-4 py-2.5 flex items-center justify-between animate-in fade-in duration-300">
    <div class="flex items-center gap-2.5 text-[13px] font-medium text-indigo-400">
      <Info class="w-4 h-4 shrink-0" />
      <span>System locked. Configure security key to enable modifications.</span>
    </div>
    <div class="flex items-center gap-4 ml-4">
      <button @click="$emit('action')" class="text-[11px] font-bold text-white bg-indigo-600 px-2.5 py-1 rounded hover:bg-indigo-500 transition-all uppercase tracking-wider">Configure</button>
      <button @click="$emit('close')" class="text-indigo-400/50 hover:text-white transition-colors">
        <X class="w-4 h-4" />
      </button>
    </div>
  </div>
</template>
