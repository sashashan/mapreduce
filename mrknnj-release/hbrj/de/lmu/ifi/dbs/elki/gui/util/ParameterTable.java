package de.lmu.ifi.dbs.elki.gui.util;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.BitSet;

import javax.swing.AbstractCellEditor;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;

import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.ClassParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.FileParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.Flag;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.Parameter;

/**
 * Class showing a table of ELKI parameters.
 * 
 * @author Erich Schubert
 */
public class ParameterTable extends JTable {
  /**
   * Serial version
   */
  private static final long serialVersionUID = 1L;

  /**
   * Color for parameters that are not optional and not yet specified.
   */
  static final Color COLOR_INCOMPLETE = new Color(0xFFCF9F);

  /**
   * Color for parameters with an invalid value.
   */
  static final Color COLOR_SYNTAX_ERROR = new Color(0xFFAFAF);

  /**
   * Color for optional parameters (with no default value)
   */
  static final Color COLOR_OPTIONAL = new Color(0xDFFFDF);

  /**
   * Color for parameters having a default value.
   */
  static final Color COLOR_DEFAULT_VALUE = new Color(0xDFDFDF);

  /**
   * The parameters we edit.
   */
  protected DynamicParameters parameters;

  /**
   * Constructor
   * 
   * @param pm Parameter Model
   * @param parameters Parameter storage
   */
  public ParameterTable(ParametersModel pm, DynamicParameters parameters) {
    super(pm);
    this.parameters = parameters;
    this.setPreferredScrollableViewportSize(new Dimension(800, 400));
    this.setFillsViewportHeight(true);
    final ColorfolRenderer colorfulRenderer = new ColorfolRenderer();
    this.setDefaultRenderer(Parameter.class, colorfulRenderer);
    this.setDefaultRenderer(String.class, colorfulRenderer);
    final AdjustingEditor editor = new AdjustingEditor();
    this.setDefaultEditor(String.class, editor);
    this.setAutoResizeMode(AUTO_RESIZE_ALL_COLUMNS);
    TableColumn col1 = this.getColumnModel().getColumn(0);
    col1.setPreferredWidth(150);
    TableColumn col2 = this.getColumnModel().getColumn(1);
    col2.setPreferredWidth(650);
  }

  /**
   * Renderer for the table that colors the entries according to their bitmask.
   * 
   * @author Erich Schubert
   */
  private class ColorfolRenderer extends DefaultTableCellRenderer {
    /**
     * Serial Version
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public ColorfolRenderer() {
      super();
    }

    @Override
    public void setValue(Object value) {
      if(value instanceof String) {
        setText((String) value);
        setToolTipText(null);
        return;
      }
      if(value instanceof DynamicParameters.Node) {
        Parameter<?, ?> o = ((DynamicParameters.Node) value).param;
        // Simulate a tree using indentation - there is no JTreeTable AFAICT
        StringBuffer buf = new StringBuffer();
        for(int i = 0; i < ((DynamicParameters.Node) value).depth; i++) {
          buf.append("  ");
        }
        buf.append(o.getOptionID().getName());
        setText(buf.toString());
        setToolTipText(o.getOptionID().getDescription());
        return;
      }
      setText("");
      setToolTipText(null);
    }

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
      Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
      if(row < parameters.size()) {
        BitSet flags = parameters.getNode(row).flags;
        // TODO: don't hardcode black - maybe mix the other colors, too?
        c.setForeground(Color.BLACK);
        if((flags.get(DynamicParameters.BIT_INVALID))) {
          c.setBackground(COLOR_SYNTAX_ERROR);
        }
        else if((flags.get(DynamicParameters.BIT_SYNTAX_ERROR))) {
          c.setBackground(COLOR_SYNTAX_ERROR);
        }
        else if((flags.get(DynamicParameters.BIT_INCOMPLETE))) {
          c.setBackground(COLOR_INCOMPLETE);
        }
        else if((flags.get(DynamicParameters.BIT_DEFAULT_VALUE))) {
          c.setBackground(COLOR_DEFAULT_VALUE);
        }
        else if((flags.get(DynamicParameters.BIT_OPTIONAL))) {
          c.setBackground(COLOR_OPTIONAL);
        }
        else {
          c.setBackground(null);
        }
      }
      return c;
    }
  }

  /**
   * Editor using a Dropdown box to offer known values to choose from.
   * 
   * @author Erich Schubert
   */
  private class DropdownEditor extends DefaultCellEditor {
    /**
     * Serial Version
     */
    private static final long serialVersionUID = 1L;

    /**
     * Combo box to use
     */
    private final JComboBox comboBox;

    /**
     * Constructor.
     * 
     * @param comboBox Combo box we're going to use
     */
    public DropdownEditor(JComboBox comboBox) {
      super(comboBox);
      this.comboBox = comboBox;
    }

    @Override
    public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
      Component c = super.getTableCellEditorComponent(table, value, isSelected, row, column);
      // remove old contents
      comboBox.removeAllItems();
      // Put the current value in first.
      Object val = table.getValueAt(row, column);
      if(val != null) {
        comboBox.addItem(val);
        comboBox.setSelectedIndex(0);
      }
      if(row < parameters.size()) {
        Parameter<?, ?> option = parameters.getNode(row).param;
        // We can do dropdown choices for class parameters
        if(option instanceof ClassParameter<?>) {
          ClassParameter<?> cp = (ClassParameter<?>) option;
          // For parameters with a default value, offer using the default
          // For optional parameters, offer not specifying them.
          if(cp.hasDefaultValue()) {
            comboBox.addItem(DynamicParameters.STRING_USE_DEFAULT);
          }
          else if(cp.isOptional()) {
            comboBox.addItem(DynamicParameters.STRING_OPTIONAL);
          }
          // Offer the shorthand version of class names.
          String prefix = cp.getRestrictionClass().getPackage().getName() + ".";
          for(Class<?> impl : cp.getKnownImplementations()) {
            String name = impl.getName();
            if(name.startsWith(prefix)) {
              comboBox.addItem(name.substring(prefix.length()));
            }
            else {
              comboBox.addItem(name);
            }
          }
        }
        // and for Flag parameters.
        else if(option instanceof Flag) {
          if(!Flag.SET.equals(val)) {
            comboBox.addItem(Flag.SET);
          }
          if(!Flag.NOT_SET.equals(val)) {
            comboBox.addItem(Flag.NOT_SET);
          }
        }
        // No completion for others
      }
      return c;
    }
  }

  /**
   * Editor for selecting input and output file and folders names
   * 
   * @author Erich Schubert
   */
  private class FileNameEditor extends AbstractCellEditor implements TableCellEditor, ActionListener {
    /**
     * Serial version number
     */
    private static final long serialVersionUID = 1L;

    /**
     * We need a panel to put our components on.
     */
    final JPanel panel = new JPanel();

    /**
     * Text field to store the name
     */
    final JTextField textfield = new JTextField();

    /**
     * The button to open the file selector
     */
    final JButton button = new JButton("...");

    /**
     * The actual file chooser
     */
    final JFileChooser fc = new JFileChooser();

    /**
     * Constructor.
     */
    public FileNameEditor() {
      button.addActionListener(this);
      panel.setLayout(new BorderLayout());
      panel.add(textfield, BorderLayout.CENTER);
      panel.add(button, BorderLayout.EAST);
    }

    /**
     * Callback from the file selector.
     */
    public void actionPerformed(@SuppressWarnings("unused") ActionEvent e) {
      int returnVal = fc.showOpenDialog(button);

      if(returnVal == JFileChooser.APPROVE_OPTION) {
        textfield.setText(fc.getSelectedFile().getPath());
      }
      else {
        // Do nothing on cancel.
      }
      fireEditingStopped();
    }

    /**
     * Delegate getCellEditorValue to the text field.
     */
    public Object getCellEditorValue() {
      return textfield.getText();
    }

    /**
     * Apply the Editor for a selected option.
     */
    public Component getTableCellEditorComponent(@SuppressWarnings("unused") JTable table, @SuppressWarnings("unused") Object value, @SuppressWarnings("unused") boolean isSelected, int row, @SuppressWarnings("unused") int column) {
      if(row < parameters.size()) {
        Parameter<?, ?> option = parameters.getNode(row).param;
        if(option instanceof FileParameter) {
          FileParameter fp = (FileParameter) option;
          File f = null;
          if(fp.isDefined()) {
            f = fp.getValue();
          }
          if(f != null) {
            String fn = f.getPath();
            textfield.setText(fn);
            fc.setSelectedFile(f);
          }
          else {
            textfield.setText("");
            fc.setSelectedFile(null);
          }
        }
      }
      return panel;
    }
  }

  /**
   * This Editor will adjust to the type of the Option: Sometimes just a plain
   * text editor, sometimes a ComboBox to offer known choices, and sometime a
   * file selector dialog.
   * 
   * TODO: class list parameters etc.
   * 
   * @author Erich Schubert
   * 
   */
  private class AdjustingEditor extends AbstractCellEditor implements TableCellEditor {
    /**
     * Serial version
     */
    private static final long serialVersionUID = 1L;

    /**
     * The dropdown editor
     */
    private final DropdownEditor dropdownEditor;

    /**
     * The plain text cell editor
     */
    private final DefaultCellEditor plaintextEditor;

    /**
     * The file selector editor
     */
    private final FileNameEditor fileNameEditor;

    /**
     * We need to remember which editor we delegated to, so we know whom to ask
     * for the value entered.
     */
    private TableCellEditor activeEditor;

    /**
     * Constructor.
     */
    public AdjustingEditor() {
      final JComboBox combobox = new JComboBox();
      combobox.setEditable(true);
      this.dropdownEditor = new DropdownEditor(combobox);
      this.plaintextEditor = new DefaultCellEditor(new JTextField());
      this.fileNameEditor = new FileNameEditor();
    }

    @Override
    public Object getCellEditorValue() {
      if(activeEditor == null) {
        return null;
      }
      return activeEditor.getCellEditorValue();
    }

    @Override
    public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
      if(row < parameters.size()) {
        Parameter<?, ?> option = parameters.getNode(row).param;
        if(option instanceof Flag) {
          activeEditor = dropdownEditor;
          return dropdownEditor.getTableCellEditorComponent(table, value, isSelected, row, column);
        }
        if(option instanceof ClassParameter<?>) {
          activeEditor = dropdownEditor;
          return dropdownEditor.getTableCellEditorComponent(table, value, isSelected, row, column);
        }
        if(option instanceof FileParameter) {
          activeEditor = fileNameEditor;
          return fileNameEditor.getTableCellEditorComponent(table, value, isSelected, row, column);
        }
      }
      activeEditor = plaintextEditor;
      return plaintextEditor.getTableCellEditorComponent(table, value, isSelected, row, column);
    }
  }
}