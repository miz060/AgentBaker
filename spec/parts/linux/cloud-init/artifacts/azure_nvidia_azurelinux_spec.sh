#!/bin/bash

Describe '10_azure_nvidia_azurelinux'
    setup() {
        TEST_DIR=$(mktemp -d)
        BOOT_DIR="${TEST_DIR}/boot"
        GRUB_LINUX_SCRIPT="${TEST_DIR}/10_linux"
        mkdir -p "${BOOT_DIR}" "${TEST_DIR}/bin"
        touch \
            "${BOOT_DIR}/vmlinuz-6.6.9-1.azl3" \
            "${BOOT_DIR}/vmlinuz-6.6.10-1.azl3" \
            "${BOOT_DIR}/vmlinuz-6.12.8-1.azl3" \
            "${BOOT_DIR}/vmlinuz-6.12.10-1.azl3" \
            "${BOOT_DIR}/vmlinuz-6.20.0-unowned"
        cat > "${GRUB_LINUX_SCRIPT}" <<'EOF'
    boot_device_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
EOF
        cat > "${TEST_DIR}/bin/rpm" <<'EOF'
#!/bin/sh
for arg do
    kernel_path=$arg
done
case "${kernel_path}" in
    *vmlinuz-6.6.*) printf 'kernel' ;;
    *vmlinuz-6.12.*) printf 'kernel-hwe' ;;
    *) exit 1 ;;
esac
EOF
        chmod +x "${TEST_DIR}/bin/rpm"
        export BOOT_DIR GRUB_LINUX_SCRIPT
        PATH="${TEST_DIR}/bin:${PATH}"
        export PATH
    }

    cleanup() {
        rm -rf "${TEST_DIR}"
    }

    BeforeEach 'setup'
    AfterEach 'cleanup'

    It 'selects the newest RPM-owned kernel from each track'
        When run script ./parts/linux/cloud-init/artifacts/10_azure_nvidia_azurelinux
        The status should be success
        The output should include 'set default="gnulinux-6.12.10-1.azl3-advanced-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"'
        The output should include 'set default="gnulinux-6.6.10-1.azl3-advanced-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"'
        The output should not include '6.20.0-unowned'
        The stderr should include 'Default HWE: 6.12.10-1.azl3, standard: 6.6.10-1.azl3'
    End

    It 'adds NVIDIA Grace kernel arguments only to the NVIDIA path'
        When run script ./parts/linux/cloud-init/artifacts/10_azure_nvidia_azurelinux
        The status should be success
        # shellcheck disable=SC2016
        The output should include 'if [ x$cpu_manufacturer = xNVIDIA ]; then'
        The output should include 'set nvidia_args="iommu.passthrough=1 irqchip.gicv3_nolpi=y arm_smmu_v3.disable_msipolling=1"'
        The stderr should include 'Default HWE: 6.12.10-1.azl3, standard: 6.6.10-1.azl3'
    End

    It 'emits no override when either kernel track is missing'
        rm -f "${BOOT_DIR}"/vmlinuz-6.12.*
        When run script ./parts/linux/cloud-init/artifacts/10_azure_nvidia_azurelinux
        The status should be success
        The output should eq ''
        The stderr should include 'Only one kernel variant (HWE or standard) found'
    End

    It 'emits no override when 10_linux cannot derive a boot device ID'
        printf '%s\n' 'boot_device_id=' > "${GRUB_LINUX_SCRIPT}"
        When run script ./parts/linux/cloud-init/artifacts/10_azure_nvidia_azurelinux
        The status should be success
        The output should eq ''
        The stderr should include "Could not determine boot_device_id from ${GRUB_LINUX_SCRIPT}"
    End
End