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
  <header class="sticky top-0 z-50 h-14 bg-bg/80 backdrop-blur-md border-b border-border-subtle">
    <div class="max-w-[1120px] mx-auto h-full px-6 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <div class="bg-accent p-1.5 rounded">
          <Zap class="w-4 h-4 text-white" />
        </div>
        <div class="flex items-center gap-2">
          <span class="text-sm font-semibold text-text-primary">PeerTube2Nostr</span>
          <span class="text-text-muted text-xs">/</span>
          <span class="text-sm font-medium text-text-muted">Dashboard</span>
        </div>
      </div>

      <div class="flex items-center gap-4">
        <Badge :variant="statusVariant">{{ status || 'Offline' }}</Badge>
        <div class="h-4 w-px bg-border-subtle mx-1"></div>
        <Button variant="secondary" size="sm" @click="$emit('configure-access')">Access</Button>
        <Button variant="primary" size="sm" @click="$emit('add-source')">
          <PlusCircle class="w-3.5 h-3.5 mr-1.5" /> Source
        </Button>
      </div>
    </div>
  </header>
</template>
