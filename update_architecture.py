#!/usr/bin/env python3
"""
Update architecture.archimate with CLI Configuration component.

This script adds:
1. CLI Configuration component (comp-cli-config)
2. Configuration File data object (do-config-file)
3. Two new relationships (rel-27, rel-28)

Run with: python3 update_architecture.py
"""

def main():
    print("Reading architecture.archimate...")
    
    # Read the original file
    with open("architecture.archimate", "r", encoding="utf-8") as f:
        content = f.read()
    
    # 1. Insert CLI Configuration component after CLI component
    cli_config_component = '''
    <!-- CLI sub-components -->
    <element xsi:type="archimate:ApplicationComponent" id="comp-cli-config"
             name="CLI Configuration"
             documentation="Handles loading, parsing, and validation of JSON/YAML configuration files. Supports CLI argument precedence and component existence validation."/>
'''
    
    content = content.replace(
        '             documentation="Cyclopts-based command-line interface. Entry point for all fintran operations. Accepts --from, --to, input file, and -o output flags."/>',
        '             documentation="Cyclopts-based command-line interface. Entry point for all fintran operations. Accepts --from, --to, input file, and -o output flags."/>' + cli_config_component
    )
    print("✓ Added CLI Configuration component (comp-cli-config)")
    
    # 2. Insert Configuration File data object after DuckDB
    config_file_data_object = '''
    <element xsi:type="archimate:DataObject" id="do-config-file"
             name="Configuration File"
             documentation="JSON or YAML file containing reader, writer, transform, and pipeline configuration. Supports CLI argument overrides."/>
'''
    
    content = content.replace(
        '             documentation=".duckdb file containing a balances table in IR schema."/>',
        '             documentation=".duckdb file containing a balances table in IR schema."/>' + config_file_data_object
    )
    print("✓ Added Configuration File data object (do-config-file)")
    
    # 3. Insert new relationships before </relationships>
    new_relationships = '''
    <!-- CLI composes CLI Configuration -->
    <relationship xsi:type="archimate:CompositionRelationship" id="rel-27"
                  source="comp-cli" target="comp-cli-config"/>

    <!-- CLI Configuration accesses Configuration File -->
    <relationship xsi:type="archimate:AccessRelationship" id="rel-28"
                  source="comp-cli-config" target="do-config-file" accessType="Read"/>

'''
    
    content = content.replace('  </relationships>', new_relationships + '  </relationships>')
    print("✓ Added 2 new relationships (rel-27, rel-28)")
    
    # Write the updated file
    with open("architecture.archimate", "w", encoding="utf-8") as f:
        f.write(content)
    
    print("\n✅ Architecture file updated successfully!")
    print("\nChanges made:")
    print("  • Added comp-cli-config component")
    print("  • Added do-config-file data object")
    print("  • Added rel-27: CLI composes CLI Configuration")
    print("  • Added rel-28: CLI Configuration accesses Configuration File")
    print("\nYou can now proceed with implementing the CLI configuration module.")

if __name__ == "__main__":
    main()
