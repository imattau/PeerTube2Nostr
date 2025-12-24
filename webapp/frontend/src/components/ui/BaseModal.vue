<script setup lang="ts">
import { X } from 'lucide-vue-next'
interface Props {
  show: boolean
  title: string
}
defineProps<Props>()
defineEmits(['close'])
</script>

<template>
  <div v-if="show" class="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
    <div class="bg-[#0f0f0f] border border-white/[0.08] w-full max-w-md rounded-xl shadow-2xl overflow-hidden animate-in zoom-in-95 duration-200">
      <div class="px-6 py-4 flex items-center justify-between border-b border-white/[0.05]">
        <h3 class="text-sm font-bold text-white uppercase tracking-wider">{{ title }}</h3>
        <button @click="$emit('close')" class="p-1 hover:bg-white/5 rounded transition-colors">
          <X class="w-4 h-4 text-slate-500" />
        </button>
      </div>
      <div class="p-6">
        <slot />
      </div>
    </div>
  </div>
</template>
