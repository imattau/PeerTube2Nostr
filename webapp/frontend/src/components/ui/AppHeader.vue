<script setup lang="ts">
import { Zap, PlusCircle } from 'lucide-vue-next'
import Badge from './Badge.vue'
import Button from './Button.vue'

interface Props {
  status: string
  statusVariant: 'success' | 'error' | 'info' | 'warning'
}
defineProps<Props>()
defineEmits(['add-source', 'configure-access'])
</script>

<template>
  <header class="sticky top-0 z-50 h-14 bg-[#050505]/80 backdrop-blur-md border-b border-white/[0.05]">
    <div class="max-w-[1120px] mx-auto h-full px-6 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <div class="bg-indigo-600 p-1 rounded">
          <Zap class="w-4 h-4 text-white fill-white" />
        </div>
        <div class="flex items-center gap-2">
          <span class="text-sm font-bold text-white tracking-tight">PeerTube2Nostr</span>
          <span class="text-slate-800 text-xs">/</span>
          <span class="text-[13px] font-medium text-slate-400">Dashboard</span>
        </div>
      </div>

      <div class="flex items-center gap-3">
        <Badge :variant="statusVariant">{{ status || 'Offline' }}</Badge>
        <div class="h-4 w-[1px] bg-white/[0.08] mx-1"></div>
        <Button variant="secondary" size="sm" @click="$emit('configure-access')">Access</Button>
        <Button variant="primary" size="sm" @click="$emit('add-source')">
          <PlusCircle class="w-3.5 h-3.5 mr-1.5" /> Source
        </Button>
      </div>
    </div>
  </header>
</template>
